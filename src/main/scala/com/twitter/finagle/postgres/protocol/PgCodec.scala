package com.twitter.finagle.postgres.protocol

import com.twitter.finagle._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.frame.FrameDecoder
import com.twitter.logging.Logger
import com.twitter.util.Future
import collection.mutable
import com.twitter.logging.Level
import com.twitter.util.StateMachine
import com.twitter.concurrent.Spool
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw

class PgCodec(user: String, password: Option[String], database: String) extends CodecFactory[PgRequest, PgResponse] {
  def server = throw new UnsupportedOperationException("client only")

  def client = Function.const {
    new Codec[PgRequest, PgResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("binary_to_packet", new PacketDecoder)
          pipeline.addLast("packet_to_backend_messages", new BackendMessageDecoder(new BackendMessageParser))
          pipeline.addLast("backend_messages_to_postgres_response", new PgResponseHandler())
          pipeline.addLast("frontend", new FrontendEncoder)
          pipeline
        }
      }

      override def prepareServiceFactory(underlying: ServiceFactory[PgRequest, PgResponse]) = {
        val errorHandling = new HandleErrorsProxy(underlying)
        new AuthenticationProxy(errorHandling, user, password, database)
      }

    }
  }

}

class HandleErrorsProxy(delegate: ServiceFactory[PgRequest, PgResponse])
  extends ServiceFactoryProxy(delegate) {

  private val logger = Logger(getClass.getName)

  override def apply(conn: ClientConnection): Future[Service[PgRequest, PgResponse]] = {
    for {
      service <- delegate.apply(conn)
    } yield HandleErrors.andThen(service)
  }

  object HandleErrors extends SimpleFilter[PgRequest, PgResponse] {
    def apply(request: PgRequest, service: Service[PgRequest, PgResponse]) =
      service.apply(request).flatMap {
        case Error(details) => Future.exception(Errors.server(details.getOrElse("")))
        case r => Future.value(r)
      }
  }

}

class AuthenticationProxy(delegate: ServiceFactory[PgRequest, PgResponse], user: String, password: Option[String], database: String)
  extends ServiceFactoryProxy(delegate) {

  private val logger = Logger(getClass.getName)

  override def apply(conn: ClientConnection): Future[Service[PgRequest, PgResponse]] = {
    for {
      service <- delegate.apply(conn)
      startupResponse <- service(Communication.request(new StartupMessage(user, database)))
      passwordResponse <- sendPassword(startupResponse, service)
      _ <- verifyResponse(passwordResponse)
    } yield service
  }

  private[this] def sendPassword(startupResponse: PgResponse, service: Service[PgRequest, PgResponse]): Future[PgResponse] = {
    logger.ifDebug("Startup response -- " + startupResponse)
    startupResponse match {

      case PasswordRequired(encoding) => password match {
        case Some(pass) =>
          val msg = encoding match {
            case ClearText => PasswordMessage(pass)
            case Md5(salt) => PasswordMessage(new String(Md5Encryptor.encrypt(user.getBytes, pass.getBytes, salt)))
          }
          service(Communication.request(msg))

        case None => Future.exception(Errors.client("Password has to be specified for md5 authentication connection"))

      }

      case r => Future.value(r)
    }
  }

  private[this] def verifyResponse(response: PgResponse): Future[Unit] = {
    response match {
      case AuthenticatedResponse(statuses, processId, secretKey) =>
        logger.ifInfo("Authenticated " + processId + " " + secretKey + "\n" + statuses)
        Future(Unit)
    }
  }

}

class FrontendEncoder extends OneToOneEncoder {
  private val logger = Logger(getClass.getName)

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
    logger.ifDebug("Encoding message " + msg)

    msg match {
      case req: PgRequest =>
        val packet = req.msg.asPacket()
        packet.encode
      case _ =>
        logger.ifDebug("Cannot convert message... Skipping")
        msg
    }

  }

}

/**
 * Aggregates `BackendMessage` to `PgResponse`.
 *
 */
class PgResponseHandler() extends SimpleChannelHandler {

  sealed trait State {

    def process: PartialFunction[BackendMessage, (Option[PgResponse], State)]

    def rollback(): State

  }

  case object AuthenticationRequired extends State {
    def process: PartialFunction[BackendMessage, (Option[PgResponse], State)] = {
      case AuthenticationCleartextPassword() => (Some(PasswordRequired(ClearText)), AuthenticationRequired)
      case AuthenticationMD5Password(salt) => (Some(PasswordRequired(Md5(salt))), AuthenticationRequired)
      case AuthenticationOk() => (None, AggregatingAuthData(Map(), -1, -1))
    }

    def rollback() = AuthenticationRequired

  }

  case object WaitingForQuery extends State {
    def process: PartialFunction[BackendMessage, (Option[PgResponse], State)] = {
      case RowDescription(fields) =>
        val fieldNames = fields.map(f => Field(f.name))
        val fieldParsers = fields.map(f => ValueParser.parserOf(f))
        val promise: Promise[Spool[Row]] = new Promise()
        (Some(ResultSet(fieldNames, promise)), SelectQuery(fieldNames, fieldParsers, promise))
      case CommandComplete(tag) => (None, UpdateQuery(tag))
    }

    def rollback() = WaitingForQuery

  }

  case class SelectQuery(fieldNames: IndexedSeq[Field], fieldParsers: IndexedSeq[ChannelBuffer => Value], promise: Promise[Spool[Row]]) extends State {
    def process: PartialFunction[BackendMessage, (Option[PgResponse], State)] = {
      case row: DataRow => processRow(row)
      case CommandComplete(_) => (None, state)
      case ReadyForQuery(_) =>
        promise.update(Return(Spool.empty))
        (None, WaitingForQuery)
    }

    private[this] def processRow(dataRow: DataRow): (Option[PgResponse], State) = {
      val row = new Row(fieldNames, dataRow.data.zip(fieldParsers).map(pair => pair._2(pair._1)))
      val tail = new Promise[Spool[Row]]
      promise.update(Return(row *:: tail))
      (None, SelectQuery(fieldNames, fieldParsers, tail))
    }

    def rollback() = {
      promise.update(Throw(Errors.server("Failed to execute select query")))
      WaitingForQuery
    }
  }

  case class UpdateQuery(tag: String) extends State {
    def process: PartialFunction[BackendMessage, (Option[PgResponse], State)] = {
      case ReadyForQuery(_) => (Some(parseTag(tag)), WaitingForQuery)
    }

    def rollback() = WaitingForQuery

    private[this] def parseTag(tag: String): QueryResponse = {
      if (tag == "CREATE TABLE") {
        OK(1)
      } else if (tag == "DROP TABLE") {
        OK(1)
      } else {
        val parts = tag.split(" ")

        parts(0) match {
          case "INSERT" => OK(parts(2).toInt)
          case "DELETE" => OK(parts(1).toInt)
          case "UPDATE" => OK(parts(1).toInt)
          case _ => throw new IllegalStateException("Unknown command complete response tag " + tag)
        }
      }
    }

  }

  case class AggregatingAuthData(statuses: Map[String, String], processId: Int, secretKey: Int) extends State {
    def addStatus(name: String, value: String) = AggregatingAuthData(statuses + (name -> value), processId, secretKey)
    def setKeyData(processId: Int, secretKey: Int) = AggregatingAuthData(statuses, processId, secretKey)

    def process: PartialFunction[BackendMessage, (Option[PgResponse], State)] = {
      case ParameterStatus(name, value) => (None, addStatus(name, value))
      case BackendKeyData(processId, secretKey) => (None, setKeyData(processId, secretKey))
      case ReadyForQuery(_) => (Some(AuthenticatedResponse(statuses, processId, secretKey)), WaitingForQuery)
    }

    def rollback() = WaitingForQuery // TODO is authentication successful here?

  }

  private val logger = Logger(getClass.getName)

  var state: State = AuthenticationRequired

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage

    message match {
      case msg: BackendMessage =>
        processMessage(msg).map(Channels.fireMessageReceived(ctx, _))

      case unsupported =>
        logger.warning("Only backend message is supported...")
        Channels.disconnect(ctx.getChannel)
    }

  }

  def processMessage(msg: BackendMessage): Option[PgResponse] = {

    val handleMessage: PartialFunction[BackendMessage, (Option[PgResponse], State)] = state.process.orElse {
      // TODO not sure that this should work correctly in case if select query failed
      case ErrorResponse(details) => (Some(Error(details)), state.rollback)
      case NoticeResponse(details) =>
        logger.ifInfo("Notice from backend " + details)
        (None, state)
      case ParameterStatus(name, value) =>
        logger.ifInfo("Param '" + name + "' has changed to '" + value + "'")
        (None, state)

      case _ => throw new UnsupportedMessage("Unknown message from backend " + msg + " for state " + state)
    }

    val (response, newState) = handleMessage(msg)

    state = newState
    response
  }

}

/**
 * Decodes `Packet` to the `BackendMessage`.
 */
class BackendMessageDecoder(val parser: BackendMessageParser) extends SimpleChannelHandler {
  private val logger = Logger(getClass.getName)

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage

    logger.ifDebug("Decoding request " + message)

    message match {
      case packet: Packet =>
        logger.ifDebug("Packet passed. Trying to parse...")

        parser.parse(packet) match {
          case Some(backendMessage) =>
            logger.ifDebug("Decoded message  " + backendMessage)
            Channels.fireMessageReceived(ctx, backendMessage)
          case None =>
            logger.warning("Cannot parse the packet. Disconnecting...")
            Channels.disconnect(ctx.getChannel)
        }

      case _ =>
        logger.warning("Only packet is supported...")
        Channels.disconnect(ctx.getChannel)
    }

  }
}

/**
 * Decodes byte stream to the `Packet`.
 */
class PacketDecoder extends FrameDecoder {
  private val logger = Logger(getClass.getName)
  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    logger.ifDebug("decoding..")
    if (buffer.readableBytes() < 5) {
      return null
    }

    buffer.markReaderIndex()
    val code: Char = buffer.readByte().asInstanceOf[Char]
    val totalLength = buffer.readInt()
    val length = totalLength - 4

    if (buffer.readableBytes() < length) {
      buffer.resetReaderIndex()
      return null
    }

    logger.ifDebug("packet with code " + code)
    new Packet(Some(code), totalLength, buffer.readSlice(length))

  }
}

class UnsupportedMessage(message: String) extends Exception(message)





