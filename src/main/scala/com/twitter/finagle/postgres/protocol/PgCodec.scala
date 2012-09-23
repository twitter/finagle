package com.twitter.finagle.postgres.protocol

import com.twitter.finagle._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.frame.FrameDecoder
import com.twitter.logging.Logger
import com.twitter.util.Future
import java.security.MessageDigest
import collection.mutable
import com.twitter.logging.Level

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

      case SingleMessageResponse(AuthenticationMD5Password(salt)) =>
        service(Communication.request(new PasswordMessage(encrypt(salt))))

      case SingleMessageResponse(AuthenticationCleartextPassword()) =>
        if (password.isEmpty) {
          // TODO throw exception
        }
        service(Communication.request(new PasswordMessage(password.get)))

      case MessageSequenceResponse(AuthenticationOk() :: _) =>
        Future.value(startupResponse)

      case _ =>
        throw new IllegalStateException("Unknown response message")
    }
  }

  private[this] def verifyResponse(response: PgResponse): Future[Unit] = {
    response match {
      case MessageSequenceResponse(AuthenticationOk() :: _) =>
        Future.value(Unit)
      case _ =>
        // TODO change exception
        Future.exception(new IllegalStateException("Cannot authenticate"))
    }
  }

  /**
   * Encodes user/password/salt information in the following way:
   * MD5(MD5(password + user) + salt)
   */
  private[this] def encrypt(salt: Array[Byte]): String = {
    val inner = MessageDigest.getInstance("MD5")

    if (password.isEmpty) {
      // TODO throw exception here
    }

    inner.update(password.get.getBytes)
    inner.update(user.getBytes)

    val outer = MessageDigest.getInstance("MD5")

    outer.update("md5".getBytes)
    outer.update(inner.digest.map(byteToHex))
    outer.update(salt)

    new String(outer.digest.map(byteToHex))
  }

  def byteToHex(b: Byte) = HexDigits((b & 0xFF) >> 4).toByte

}

class FrontendEncoder extends OneToOneEncoder {
  private val logger = Logger(getClass.getName)

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
    logger.ifDebug("Encoding message " + msg)

    msg match {
      case req: PgRequest =>
        req.msg.encode()
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
  trait Mode

  case object OneMessagePerRequest extends Mode

  case class MessageSequenceMode(messages: mutable.MutableList[BackendMessage], termintation: BackendMessage => Boolean) extends Mode

  private val logger = Logger(getClass.getName)

  private var mode: Mode = OneMessagePerRequest

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
    mode match {
      case OneMessagePerRequest =>

        msg match {

          case AuthenticationOk() =>
            mode = new MessageSequenceMode((new mutable.MutableList() += msg), newMsg => {
              newMsg match {
                case ReadyForQuery(_) =>
                  true
                case ErrorResponse(_) =>
                  true
                case _ =>
                  false
              }
            })
            None

          case RowDescription(_) =>
            mode = new MessageSequenceMode((new mutable.MutableList() += msg), newMsg => {
              newMsg match {
                case ReadyForQuery(_) =>
                  true
                case ErrorResponse(_) =>
                  true
                case _ =>
                  false
              }
            })
            None
          case _ =>
            Some(SingleMessageResponse(msg))
        }

      case MessageSequenceMode(messages, termination) =>
        messages += msg
        if (termination(msg)) {
          mode = OneMessagePerRequest
          Some(Communication.sequence(messages.toList))
        } else {
          None
        }
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
    logger.debug("decoding..")
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

    logger.debug("packet with code " + code)
    new Packet(code, totalLength, buffer.readSlice(length))

  }
}

