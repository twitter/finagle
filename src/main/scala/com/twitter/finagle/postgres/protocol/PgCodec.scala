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

      override def prepareServiceFactory(underlying: ServiceFactory[PgRequest, PgResponse]) =
        new AuthenticationProxy(underlying, user, password, database)

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

      case PasswordRequired(encoding) =>
        if (password.isEmpty) {
          Future.exception(new IllegalArgumentException("Password has to be specified for md5 authentication connection"))
        } else {
          val msg = encoding match {
            case ClearText => PasswordMessage(password.get)
            case Md5(salt) => PasswordMessage(new String(Md5Encriptor.encript(user.getBytes, password.get.getBytes, salt)))
          }
          service(Communication.request(msg))
        }

      case r => Future.value(r)
    }
  }

  private[this] def verifyResponse(response: PgResponse): Future[Unit] = {
    response match {
      case AuthenticatedResponse(statuses, processId, secretKey) =>
        logger.ifInfo("Authenticated " + processId + " " + secretKey + "\n" + statuses)
        Future(Unit)
      case _ =>
        // TODO change exception
        Future.exception(new IllegalStateException("Cannot authenticate"))
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
class PgResponseHandler() extends SimpleChannelHandler with StateMachine {

  case object AuthenticationRequired extends State

  case object WaitingForQuery extends State

  case class SelectQuery(fieldNames: IndexedSeq[Field], fieldParsers: IndexedSeq[ChannelBuffer => Value], promise: Promise[Spool[Row]]) extends State

  case class UpdateQuery(tag: String) extends State

  case class AggregatingAuthData(statuses: Map[String, String], processId: Int, secretKey: Int) extends State {
    def addStatus(name: String, value: String) = AggregatingAuthData(statuses + (name -> value), processId, secretKey)
    def setKeyData(processId: Int, secretKey: Int) = AggregatingAuthData(statuses, processId, secretKey)
  }

  private val logger = Logger(getClass.getName)

  state = AuthenticationRequired

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
    val (response, newState) = state match {

      case AuthenticationRequired =>
        handleAuthentication(msg)

      case WaitingForQuery =>
        handleQueryResponse(msg)

      case s: UpdateQuery =>
        handleUpdateQuery(msg, s)

      case s: SelectQuery =>
        processSelectQuery(msg, s)

      case s: AggregatingAuthData =>
        accumulateAuthData(msg, s)
    }
    state = newState
    response
  }

  private[this] def processSelectQuery(msg: BackendMessage, state: SelectQuery): (Option[PgResponse], State) = {
    logger.ifDebug("handling select data. [msg " + msg + " state " + state + "]")
    msg match {
      case row: DataRow => processRow(row, state)
      case CommandComplete(_) => (None, state)
      case ReadyForQuery(_) =>
        logger.ifDebug("Select query comleted state '" + state + "'")
        state.promise.update(Return(Spool.empty))
        (None, WaitingForQuery)
      case ErrorResponse(details) =>
        logger.ifDebug("Select query failed state '" + state + "'")
        state.promise.update(Throw(new IllegalStateException("TODO: Introduce exception")))
        (Some(Error(details)), WaitingForQuery)
    }
  }

  private[this] def processRow(dataRow: DataRow, state: SelectQuery): (Option[PgResponse], State) = {
    logger.ifDebug("Processing row '" + state + "'")

    val SelectQuery(fieldNames, fieldParsers, promise) = state
    val row = new Row(fieldNames, dataRow.data.zip(fieldParsers).map(pair => pair._2(pair._1)))
    val tail = new Promise[Spool[Row]]
    promise.update(Return(row *:: tail))
    (None, SelectQuery(fieldNames, fieldParsers, tail))
  }

  private[this] def handleQueryResponse(msg: BackendMessage): (Option[PgResponse], State) = {
    logger.ifDebug("handling query response. [msg " + msg + " state " + state + "]")
    msg match {
      case RowDescription(fields) =>
        val fieldNames = fields.map(f => Field(f.name))
        val fieldParsers = fields.map(f => ValueParser.parserOf(f))
        val promise: Promise[Spool[Row]] = new Promise()
        (Some(ResultSet(fieldNames, promise)), SelectQuery(fieldNames, fieldParsers, promise))
      case CommandComplete(tag) => (None, UpdateQuery(tag))
      case ErrorResponse(details) => (Some(Error(details)), WaitingForQuery)
    }
  }

  private[this] def handleUpdateQuery(msg: BackendMessage, state: UpdateQuery): (Option[PgResponse], State) = {
    logger.ifDebug("handling update query response. [msg " + msg + " state " + state + "]")
    msg match {
      case ReadyForQuery(_) => (Some(parseTag(state.tag)), WaitingForQuery)
      case ErrorResponse(details) => (Some(Error(details)), WaitingForQuery)
    }
  }

  private[this] def handleAuthentication(msg: BackendMessage): (Option[PgResponse], State) = {
    logger.ifDebug("handling authentication. [msg " + msg + " state " + state + "]")
    msg match {
      case AuthenticationCleartextPassword() => (Some(PasswordRequired(ClearText)), AuthenticationRequired)
      case AuthenticationMD5Password(salt) => (Some(PasswordRequired(Md5(salt))), AuthenticationRequired)
      case AuthenticationOk() => (None, AggregatingAuthData(Map(), -1, -1))
      case ErrorResponse(details) => (Some(Error(details)), AuthenticationRequired)
    }
  }

  private[this] def accumulateAuthData(msg: BackendMessage, state: AggregatingAuthData): (Option[PgResponse], State) = {
    logger.ifDebug("aggregating authentication data. [msg " + msg + " state " + state + "]")
    msg match {
      case ParameterStatus(name, value) => (None, state.addStatus(name, value))
      case BackendKeyData(processId, secretKey) => (None, state.setKeyData(processId, secretKey))
      case ReadyForQuery(_) => (Some(AuthenticatedResponse(state.statuses, state.processId, state.secretKey)), WaitingForQuery)
      case ErrorResponse(details) => (Some(Error(details)), AuthenticationRequired)
    }
  }

  private[this] def parseTag(tag: String): QueryResponse = {
    val parts = tag.split(" ")

    parts(0) match {
      case "INSERT" => OK(parts(2).toInt)
      case "DELETE" => OK(parts(1).toInt)
      case "UPDATE" => OK(parts(1).toInt)
      case _ => throw new IllegalStateException("Unknown command complete response tag " + tag)
    }
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

        val result = parser.parse(packet)

        if (result.isDefined) {
          val backendMessage = result.get

          logger.ifDebug("Decoded message  " + backendMessage)

          Channels.fireMessageReceived(ctx, backendMessage)

        } else {
          logger.warning("Cannot parse the packet. Disconnecting...")
          Channels.disconnect(ctx.getChannel)
        }

      case unsupported =>
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





