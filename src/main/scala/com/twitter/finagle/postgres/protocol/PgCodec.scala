package com.twitter.finagle.postgres.protocol

import com.twitter.finagle._
import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.frame.FrameDecoder
import com.twitter.logging.Logger
import com.twitter.util.Future

class PgCodec(user: String, password: Option[String], database: String) extends CodecFactory[PgRequest, PgResponse] {
  def server = throw new UnsupportedOperationException("client only")

  def client = Function.const {
    new Codec[PgRequest, PgResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("binary_to_packet", new PacketDecoder)
          pipeline.addLast("packet_to_backend_messages", new BackendMessageDecoder(new BackendMessageParser))
          pipeline.addLast("backend_messages_to_postgres_response", new PgClientChannelHandler())
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
      startupResponse <- service(PgRequest(new StartupMessage(user, database)))
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
          service(PgRequest(msg))

        case None => Future.exception(Errors.client("Password has to be specified for authenticated connection"))

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

/**
 * PgRequest -> PgResponse
 */
class PgClientChannelHandler extends SimpleChannelHandler {

  private[this] val logger = Logger(getClass.getName)

  private[this] val connection = new Connection()

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage

    message match {
      case msg: BackendMessage =>
        connection.receive(msg).map {
          Channels.fireMessageReceived(ctx, _)
        }

      case unsupported =>
        logger.warning("Only backend message is supported...")
        Channels.disconnect(ctx.getChannel)
    }

  }

  override def writeRequested(ctx: ChannelHandlerContext, event: MessageEvent) = {
    logger.ifDebug("Encoding message " + event.getMessage)

    val buf = event.getMessage match {
      case PgRequest(msg, flush) =>
        // TODO this could be extracted as a separate handler
        val packet = msg.asPacket()
        val c = ChannelBuffers.dynamicBuffer()
        c.writeBytes(packet.encode)
        if (flush) {
          c.writeBytes(Flush.asPacket.encode)
        }
        connection.send(msg)
        c
      case _ =>
        logger.ifDebug("Cannot convert message... Skipping")
        event.getMessage
    }
    Channels.write(ctx, event.getFuture, buf, event.getRemoteAddress)
  }

}






