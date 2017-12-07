package com.twitter.finagle.postgres.codec

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.ClosedChannelException

import com.twitter.finagle._
import com.twitter.finagle.postgres.ResultSet
import com.twitter.finagle.postgres.connection.{AuthenticationRequired, Connection, RequestingSsl, WrongStateForEvent}
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values.Md5Encryptor
// import com.twitter.finagle.ssl.SessionVerifier
import com.twitter.finagle.ssl.client.{ SslClientConfiguration, SslClientEngineFactory, SslClientSessionVerifier }
import com.twitter.logging.Logger
import com.twitter.util.Future
import javax.net.ssl.{SSLContext, SSLEngine, TrustManagerFactory}

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.ssl.util.InsecureTrustManagerFactory
import org.jboss.netty.handler.ssl.{SslContext, SslHandler}
import scala.collection.mutable

import com.sun.corba.se.impl.protocol.RequestCanceledException
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.transport.Transport

/*
 * Filter that converts exceptions into ServerErrors.
 */
class HandleErrorsProxy(
    delegate: ServiceFactory[PgRequest, PgResponse]) extends ServiceFactoryProxy(delegate) {

  override def apply(conn: ClientConnection): Future[Service[PgRequest, PgResponse]] = {
    for {
      service <- delegate.apply(conn)
    } yield HandleErrors.andThen(service)
  }

  object HandleErrors extends SimpleFilter[PgRequest, PgResponse] {

    def apply(request: PgRequest, service: Service[PgRequest, PgResponse]) = {
      service.apply(request).flatMap {
        case Error(msg, severity, sqlState, detail, hint, position) =>
          Future.exception(Errors.server(msg.getOrElse("unknown failure"), Some(request), severity, sqlState, detail, hint, position))
        case Terminated =>
          Future.exception(new ChannelClosedException())
        case r => Future.value(r)
      }
    }
  }
}

/*
 * Filter that does password authentication before issuing requests.
 */
class AuthenticationProxy(
    delegate: ServiceFactory[PgRequest, PgResponse],
    user: String, password: Option[String],
    database: String,
    useSsl: Boolean) extends ServiceFactoryProxy(delegate) {
  private val logger = Logger(getClass.getName)

  override def apply(conn: ClientConnection): Future[Service[PgRequest, PgResponse]] = {
    for {
      service <- delegate.apply(conn)
      optionalSslResponse <- sendSslRequest(service)
      _ <- handleSslResponse(optionalSslResponse)
      startupResponse <- service(PgRequest(StartupMessage(user, database)))
      passwordResponse <- sendPassword(startupResponse, service)
      _ <- verifyResponse(passwordResponse)
    } yield service
  }

  private[this] def sendSslRequest(service: Service[PgRequest, PgResponse]): Future[Option[PgResponse]] = {
    if (useSsl) {
      service(PgRequest(new SslRequestMessage)).map { response => Some(response) }
    } else {
      Future.value(None)
    }
  }

  private[this] def handleSslResponse(optionalSslResponse: Option[PgResponse]): Future[Unit] = {
    logger.ifDebug("SSL response: %s".format(optionalSslResponse))

    if (useSsl && (optionalSslResponse contains SslNotSupportedResponse)) {
      throw Errors.server("SSL requested by server doesn't support it")
    } else {
      Future(Unit)
    }
  }

  private[this] def sendPassword(
      startupResponse: PgResponse, service: Service[PgRequest, PgResponse]): Future[PgResponse] = {
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
        logger.ifDebug("Authenticated: %d %d\n%s".format(processId, secretKey, statuses))
        Future(Unit)
    }
  }
}


/*
 * Decodes a Packet into a BackendMessage.
 */
class BackendMessageDecoder(val parser: BackendMessageParser) extends SimpleChannelHandler {
  private val logger = Logger(getClass.getName)

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage

    message match {
      case packet: Packet =>
        parser.parse(packet) match {
          case Some(backendMessage) =>
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

/*
 * Decodes a byte stream into a Packet.
 */
class PacketDecoder(@volatile var inSslNegotation: Boolean) extends FrameDecoder {
  private val logger = Logger(getClass.getName)

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    if (inSslNegotation && buffer.readableBytes() >= 1) {
      val SslCode: Char = buffer.readByte().asInstanceOf[Char]

      logger.ifDebug("Got ssl negotiation char packet: %s".format(SslCode))

      inSslNegotation = false

      new Packet(Some(SslCode), 1, null, true)
    } else if (buffer.readableBytes() < 5) {
      null
    } else {
      buffer.markReaderIndex()
      val code: Char = buffer.readByte().asInstanceOf[Char]

      val totalLength = buffer.readInt()
      val length = totalLength - 4

      if (buffer.readableBytes() < length) {
        buffer.resetReaderIndex()
        return null
      }

      val packet = new Packet(Some(code), totalLength, buffer.readSlice(length))

      packet
    }
  }
}

/*
 * Map PgRequest to PgResponse.
 */
class PgClientChannelHandler(
  sslEngineFactory: SslClientEngineFactory,
  sslConfig: Option[SslClientConfiguration],
  val useSsl: Boolean
) extends SimpleChannelHandler {
  private[this] val logger = Logger(getClass.getName)
  private[this] val connection = {
    if (useSsl) {
      new Connection(startState = RequestingSsl)
    } else {
      new Connection(startState = AuthenticationRequired)
    }
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    logger.ifDebug("Detected channel disconnected!")

    super.channelDisconnected(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage

    message match {
      case SwitchToSsl =>
        logger.ifDebug("Got switchToSSL message; adding ssl handler into pipeline")

        val pipeline = ctx.getPipeline

        val addr = ctx.getChannel.getRemoteAddress
        val inetAddr = addr match {
          case i: InetSocketAddress => Some(i)
          case _ => None
        }

        val engine = inetAddr.map(inet =>
          sslConfig.map(sslEngineFactory(Address(inet), _)).getOrElse(Ssl.client(inet.getHostString, inet.getPort))
        )
          .getOrElse(Ssl.client())
          .self

        engine.setUseClientMode(true)

        val sslHandler = new SslHandler(engine)
        pipeline.addFirst("ssl", sslHandler)

        val verifier = SslClientSessionVerifier.AlwaysValid

        connection.receive(SwitchToSsl).foreach {
          Channels.fireMessageReceived(ctx, _)
        }
      case msg: BackendMessage =>
        try {
          connection.receive(msg).foreach {
            Channels.fireMessageReceived(ctx, _)
          }
        } catch {
          case err @ WrongStateForEvent(evt, state) =>
            logger.error(s"Could not handle event $evt while in state $state; connection will be terminated", err)
            Channels.write(ctx.getChannel, Terminate.asPacket().encode())
            Channels.fireExceptionCaught(ctx, err)
        }
      case unsupported =>
        logger.warning("Only backend messages are supported...")
        Channels.close(ctx.getChannel)
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, event: MessageEvent) = {
    val (buf, out) = event.getMessage match {
      case PgRequest(msg, flush) =>
        val packet = msg.asPacket()
        val c = ChannelBuffers.dynamicBuffer()

        c.writeBytes(packet.encode())

        if (flush) {
          c.writeBytes(Flush.asPacket().encode())
        }

        try {
          (Some(c), connection.send(msg))
        } catch {
          case err @ WrongStateForEvent(evt, state) =>
            logger.error(s"Could not handle event $evt while in state $state; connection will be terminated", err)
            Channels.fireExceptionCaught(ctx, err)
            (None, Some(com.twitter.finagle.postgres.messages.Terminated))
        }

      case buffer: ChannelBuffer =>
        (Some(buffer), None)

      case other =>
        logger.warning(s"Cannot convert message of type ${other.getClass.getName}... Skipping")
        (Some(event.getMessage), None)
    }

    buf.filter(_ => ctx.getChannel.isOpen) foreach {
      bytes => Channels.write(ctx, event.getFuture, bytes, event.getRemoteAddress)
    }
    out collect {
      case term @ com.twitter.finagle.postgres.messages.Terminated =>
        Channels.close(ctx.getChannel)
        Channels.fireMessageReceived(ctx, term)
    }
  }
}






