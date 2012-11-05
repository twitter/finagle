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
        req.msg.asPacket().encode
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

  case object OneMessagePerRequest extends State

  case object AuthenticationRequired extends State

  case class MessageSequenceMode(messages: mutable.MutableList[BackendMessage], termintation: BackendMessage => Boolean) extends State

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

  private[this] def commonTermination(msg: BackendMessage): Boolean =
    msg match {
      case ReadyForQuery(_) =>
        true
      case ErrorResponse(_) =>
        true
      case _ =>
        false
    }

  def processMessage(msg: BackendMessage): Option[PgResponse] = {
    state match {

      case AuthenticationRequired =>
        val (response, newState) = handleAuthentication(msg)
        state = newState
        response
      case OneMessagePerRequest =>

        msg match {

          case RowDescription(_) =>
            state = new MessageSequenceMode((new mutable.MutableList() += msg), commonTermination)
            None
          case CommandComplete(_) =>
            state = new MessageSequenceMode((new mutable.MutableList() += msg), commonTermination)
            None
          case _ =>
            Some(SingleMessageResponse(msg))
        }

      case s @ AggregatingAuthData(_, _, _) =>
        val (response, newState) = aggregateAuthData(msg, s)
        state = newState
        response

      case MessageSequenceMode(messages, termination) =>
        messages += msg
        if (termination(msg)) {
          state = OneMessagePerRequest
          Some(Communication.sequence(messages.toList))
        } else {
          None
        }
    }
  }

  def handleAuthentication(msg: BackendMessage): (Option[PgResponse], State) = {
    logger.ifDebug("handling authentication")
    msg match {
      case AuthenticationCleartextPassword() => (Some(PasswordRequired(ClearText)), AuthenticationRequired)
      case AuthenticationMD5Password(salt) => (Some(PasswordRequired(Md5(salt))), AuthenticationRequired)
      case AuthenticationOk() => (None, AggregatingAuthData(Map(), -1, -1))
      case ErrorResponse(details) => (Some(Error(details)), AuthenticationRequired)
    }
  }

  def aggregateAuthData(msg: BackendMessage, state: AggregatingAuthData): (Option[PgResponse], State) = {
    logger.ifDebug("aggregating authentication data")
    msg match {
      case ParameterStatus(name, value) => (None, state.addStatus(name, value))
      case BackendKeyData(processId, secretKey) => (None, state.setKeyData(processId, secretKey))
      case ReadyForQuery(_) => (Some(AuthenticatedResponse(state.statuses, state.processId, state.secretKey)), OneMessagePerRequest)
      case ErrorResponse(details) => (Some(Error(details)), AuthenticationRequired)
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



