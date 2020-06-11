package com.twitter.finagle.postgres.codec

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.ClosedChannelException

import com.twitter.finagle._
import com.twitter.finagle.postgres.ResultSet
import com.twitter.finagle.postgres.connection.{AuthenticationRequired, Connection, RequestingSsl, WrongStateForEvent}
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values.Md5Encryptor
import com.twitter.finagle.ssl.client.{ SslClientConfiguration, SslClientEngineFactory, SslClientSessionVerifier }
import com.twitter.logging.Logger
import com.twitter.util.{ Future, Try }
import javax.net.ssl.{SSLContext, SSLEngine, SSLSession, TrustManagerFactory}

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.handler.ssl.{SslContext, SslHandler}
import io.netty.util.concurrent._
import scala.collection.mutable

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
        case e: Error =>
          Future.exception(Errors.server(e, Some(request)))
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
      Future({})
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
        Future({})
    }
  }
}


/*
 * Decodes a Packet into a BackendMessage.
 */
class BackendMessageDecoder(val parser: BackendMessageParser) extends ChannelInboundHandlerAdapter {
  private val logger = Logger(getClass.getName)

  override def channelRead(ctx: ChannelHandlerContext, message: AnyRef) {
    message match {
      case packet: Packet =>
        parser.parse(packet) match {
          case Some(backendMessage) =>
            ctx.fireChannelRead(backendMessage)
          case None =>
            ctx.channel().disconnect()
        }

      case _ =>
        logger.warning("Only packet is supported...")
        ctx.channel().disconnect()
    }
  }
}

/*
 * Decodes a byte stream into a Packet.
 */
class PacketDecoder(@volatile var inSslNegotation: Boolean) extends ByteToMessageDecoder {
  private val logger = Logger(getClass.getName)

  def decode(ctx: ChannelHandlerContext, buffer: ByteBuf, out: java.util.List[AnyRef]): Unit = {
    if (inSslNegotation && buffer.readableBytes() >= 1) {
      val SslCode: Char = buffer.readByte().asInstanceOf[Char]

      logger.ifDebug("Got ssl negotiation char packet: %s".format(SslCode))

      inSslNegotation = false

      buffer.retain()
      out.add(new Packet(Some(SslCode), 1, null, true))
    } else if (buffer.readableBytes() < 5) {
    } else {
      buffer.markReaderIndex()
      val code: Char = buffer.readByte().asInstanceOf[Char]

      val totalLength = buffer.readInt()
      val length = totalLength - 4

      if (buffer.readableBytes() < length) {
        buffer.resetReaderIndex()
      } else {
        val packet = new Packet(Some(code), totalLength, buffer.readSlice(length))
        buffer.retain()
        out.add(packet)
      }
    }
  }
}

/*
 * Map PgRequest to PgResponse.
 */
class PgClientChannelHandler(
  sslEngineFactory: SslClientEngineFactory,
  sessionVerifier: SslClientSessionVerifier,
  sslConfig: Option[SslClientConfiguration],
  val useSsl: Boolean
) extends ChannelDuplexHandler {
  private[this] val logger = Logger(getClass.getName)
  private[this] val connection = {
    if (useSsl) {
      new Connection(startState = RequestingSsl)
    } else {
      new Connection(startState = AuthenticationRequired) 
    }
  }

  override def channelInactive(ctx: ChannelHandlerContext) {
    logger.ifDebug("Detected channel disconnected!")
    super.channelInactive(ctx)
  }

  override def channelRead(ctx: ChannelHandlerContext, message: AnyRef) {
    message match {
      case SwitchToSsl =>
        logger.ifDebug("Got switchToSSL message; adding ssl handler into pipeline")

        val pipeline = ctx.pipeline()

        ctx.channel().remoteAddress() match {
          case i: InetSocketAddress =>
            val address = Address(i)
            val config = sslConfig.getOrElse(SslClientConfiguration(hostname = Some(i.getHostString)))
            val verifier = (s: SSLSession) => sessionVerifier(address, config, s)

            val engine = sslEngineFactory(address, config).self
            engine.setUseClientMode(true)

            val sslHandler = new SslHandler(engine)
            pipeline.addFirst("ssl", sslHandler)

            sslHandler.handshakeFuture().addListener(new FutureListener[Channel] {
              override def operationComplete(f: io.netty.util.concurrent.Future[Channel]): Unit = {
                if (!Try(verifier(engine.getSession)).onFailure { err =>
                  logger.error(err, "Exception thrown during SSL session verification")
                }.getOrElse(false)) {
                  logger.error("SSL session verification failed")
                  ctx.channel().close()
                }
              }
            })

            connection.receive(SwitchToSsl).foreach {
              ctx.fireChannelRead(_)
            }

          case _ =>
            ctx.fireExceptionCaught(new Exception("Unsupported socket address for SSL"))
        }
      case msg: BackendMessage =>
        try {
          connection.receive(msg).foreach {
            ctx.fireChannelRead(_)
          }
        } catch {
          case err @ WrongStateForEvent(evt, state) =>
            logger.error(s"Could not handle event $evt while in state $state; connection will be terminated", err)
            ctx.channel.write(Terminate.asPacket().encode())
            ctx.fireExceptionCaught(err)
        }
      case unsupported =>
        logger.warning("Only backend messages are supported...")
        ctx.channel().close()
    }
  }

  override def write(ctx: ChannelHandlerContext, message: AnyRef, promise: ChannelPromise) = {
    val (buf, out) = message match {
      case PgRequest(msg, flush) =>
        val packet = msg.asPacket()
        val c = Unpooled.buffer()

        c.writeBytes(packet.encode())

        if (flush) {
          c.writeBytes(Flush.asPacket().encode())
        }

        try {
          (Some(c), connection.send(msg))
        } catch {
          case err @ WrongStateForEvent(evt, state) =>
            logger.error(s"Could not handle event $evt while in state $state; connection will be terminated", err)
            ctx.fireExceptionCaught(err)
            (None, Some(com.twitter.finagle.postgres.messages.Terminated))
        }

      case buffer: ByteBuf =>
        (Some(buffer), None)

      case other =>
        logger.warning(s"Cannot convert message of type ${other.getClass.getName}... Skipping")
        (Some(message), None)
    }

    buf.filter(_ => ctx.channel.isOpen) foreach {
      bytes => ctx.write(bytes, promise)
    }
    out collect {
      case term @ com.twitter.finagle.postgres.messages.Terminated =>
        ctx.channel().close()
        ctx.fireChannelRead(term)
    }
  }
}






