package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.Address
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.SslException
import com.twitter.finagle.SslVerificationFailedException
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.server.SslServerSessionVerifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.HasLogLevel
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.GenericFutureListener
import java.security.cert.X509Certificate
import javax.net.ssl.SSLPeerUnverifiedException
import scala.util.control.NonFatal
import io.netty.util.concurrent.{Future => NettyFuture}
import javax.net.ssl.SSLSession
import scala.util.control.NonFatal

/**
 * Delays `channelActive` event until the TLS handshake is successfully finished
 * and verified.
 */
private[netty4] class SslServerVerificationHandler(
  sslHandler: SslHandler,
  remoteAddress: Address,
  config: SslServerConfiguration,
  sessionVerifier: SslServerSessionVerifier,
  statsReceiver: StatsReceiver)
    extends ChannelInboundHandlerAdapter { self =>

  private[this] val log = Logger.get(getClass)
  private[this] val onHandshakeComplete = Promise[Unit]()
  private[this] val sslExceptionStats = statsReceiver.scope("ssl_exn")

  private[this] def verifySession(session: SSLSession, ctx: ChannelHandlerContext): Unit = {
    // Clean up ThreadLocal immediately for successful handshakes
    // This prevents leaking the service identifier to subsequent requests on the same thread
    ServiceIdentifierContext.clear()

    try {
      if (sessionVerifier(remoteAddress, config, session)) {
        ctx.pipeline.remove(self)
        onHandshakeComplete.setDone()
      } else {
        val addr = Option(ctx.channel.remoteAddress)
        ctx.close()
        onHandshakeComplete.updateIfEmpty(Throw(new SslVerificationFailedException(None, addr)))
      }
    } catch {
      case NonFatal(e) =>
        ctx.close()
        val addr = Option(ctx.channel.remoteAddress)
        onHandshakeComplete.updateIfEmpty(Throw(new SslVerificationFailedException(Some(e), addr)))
    }
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    onHandshakeComplete.respond {
      case Return(_) =>
        ctx.fireChannelActive()
      case _ =>
    }

    if (!ctx.channel().config().isAutoRead) {
      ctx.read()
    }
  }

  private[this] def extractServiceIdentifier(): Option[String] = {
    // Primary path: Get the service identifier from the trust manager context.
    // The TrustManager captures this during certificate verification, before validation occurs.
    // This allows us to track the client identity even when validation fails (e.g., PKIX errors).
    val capturedId = ServiceIdentifierContext.get()
    ServiceIdentifierContext.clear() // Clean up after retrieval

    if (capturedId.isDefined) {
      return capturedId
    }

    // Fallback path: Try to extract from the SSL session directly (for tests or edge cases).
    // This won't work for PKIX failures in production (cert is discarded), but works in tests.
    try {
      val session = sslHandler.engine().getSession
      val certs = session.getPeerCertificates
      if (certs != null && certs.nonEmpty) {
        val x509Certs = certs.map(_.asInstanceOf[X509Certificate])
        return ServiceIdentifierExtractor.extractServiceIdentifier(x509Certs)
      }
    } catch {
      case _: SSLPeerUnverifiedException =>
      // Expected for cert validation failures in production
      case NonFatal(_) =>
      // Ignore other extraction errors
    }

    // No certificate available
    Some("no_peer_cert")
  }

  private[this] def trackHandshakeFailure(cause: Throwable): Unit = {
    val serviceId = extractServiceIdentifier().getOrElse("unknown")
    // Sanitize service identifier for use in metric names
    val sanitized = serviceId.replaceAll("[^a-zA-Z0-9:_-]", "_")

    // Log every time we increment the counter to debug potential double-counting
    log.warning(
      s"Incrementing ssl_exn counter for service '$serviceId' from $remoteAddress: ${cause.getClass.getSimpleName}: ${cause.getMessage}")

    sslExceptionStats.counter(sanitized).incr()

    if (log.isLoggable(Level.DEBUG)) {
      log.debug(
        s"SSL handshake failed for service identifier '$serviceId' from $remoteAddress: ${cause.getMessage}")
    }
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    sslHandler
      .handshakeFuture()
      .addListener(new GenericFutureListener[NettyFuture[Channel]] {
        def operationComplete(f: NettyFuture[Channel]): Unit = {
          if (f.isSuccess) {
            val session = sslHandler.engine().getSession
            verifySession(session, ctx)
          } else if (f.isCancelled) {
            trackHandshakeFailure(new InterruptedSslException())
            ctx.close()
            onHandshakeComplete.updateIfEmpty(Throw(new InterruptedSslException()))
          } else {
            trackHandshakeFailure(f.cause)
            ctx.close()
            onHandshakeComplete.updateIfEmpty(Throw(new HandshakeFailureException(f.cause)))
          }
        }
      })

    super.handlerAdded(ctx)
  }
}

/**
 * Indicates that the SslHandler was interrupted while it was trying to complete the TLS handshake.
 */
private[netty4] class InterruptedSslException(val flags: Long = FailureFlags.Empty)
    extends SslException(None, None)
    with FailureFlags[InterruptedSslException]
    with HasLogLevel {

  override def logLevel: Level = Level.WARNING
  protected def copyWithFlags(flags: Long): InterruptedSslException =
    new InterruptedSslException(flags)
}

private[netty4] class HandshakeFailureException(
  exn: Throwable,
  val flags: Long = FailureFlags.Empty)
    extends Exception("Failed to complete the TLS handshake.", exn)
    with FailureFlags[HandshakeFailureException]
    with HasLogLevel {
  def logLevel: Level = Level.WARNING
  protected def copyWithFlags(flags: Long): HandshakeFailureException =
    new HandshakeFailureException(exn, flags)
}
