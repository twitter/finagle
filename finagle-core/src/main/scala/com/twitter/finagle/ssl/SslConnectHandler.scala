package com.twitter.finagle.ssl

import java.util.concurrent.atomic.AtomicReference
import sun.security.util.HostnameChecker
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession

import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler

import com.twitter.util.Try
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.{
  InconsistentStateException, ChannelClosedException,
  SslHandshakeException,
  SslHostVerificationException}

/**
 * Handle client-side SSL connections:
 *
 *	1.  by delaying the upstream connect until the SSL handshake
 *	is complete (so that we don't send data through a connection
 *	we may later deem invalid), and
 *	2.  optionally performing hostname validation
 */
class SslConnectHandler(
  sslHandler: SslHandler,
  sessionError: SSLSession => Option[Throwable] = Function.const(None)
) extends SimpleChannelHandler
{
  private[this] val connectFuture = new AtomicReference[ChannelFuture](null)

  private[this] def fail(c: Channel, t: Throwable) {
    Option(connectFuture.get) foreach { _.setFailure(t) }
    Channels.close(c)
  }

  override def connectRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    e match {
      case de: DownstreamChannelStateEvent =>
        if (!connectFuture.compareAndSet(null, e.getFuture)) {
          fail(ctx.getChannel, new InconsistentStateException)
          return
        }

        // proxy cancellation
        val wrappedConnectFuture = Channels.future(de.getChannel, true)
        de.getFuture onCancellation { wrappedConnectFuture.cancel() }

        val wrappedEvent = new DownstreamChannelStateEvent(
          de.getChannel, wrappedConnectFuture,
          de.getState, de.getValue)

        super.connectRequested(ctx, wrappedEvent)

      case _ =>
        fail(ctx.getChannel, new InconsistentStateException)
    }
  }

  // we delay propagating connection upstream until we've completed the handshake.
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    if (connectFuture.get eq null) {
      fail(ctx.getChannel, new InconsistentStateException)
      return
    }

    // proxy cancellations again.
    connectFuture.get.onCancellation {
      fail(ctx.getChannel, new ChannelClosedException)
    }

    sslHandler.handshake() {
      case Ok(_) =>
        sessionError(sslHandler.getEngine.getSession) match {
          case Some(t) =>
            fail(ctx.getChannel, t)
          case None =>
            connectFuture.get.setSuccess()
            super.channelConnected(ctx, e)
        }

      case Error(t) =>
        fail(ctx.getChannel, new SslHandshakeException(t))

      case Cancelled =>
        fail(ctx.getChannel, new InconsistentStateException)
    }
  }
}

object SslConnectHandler {
  /**
   * Run hostname verification on the session.  This will fail with a
   * {{SslHostVerificationException}} if the certificate is invalid
   * for the given session.
   *
   * This uses {{sun.security.util.HostnameChecker}}.  Any bugs are
   * theirs.
   */
  def sessionHostnameVerifier(hostname: String)(session: SSLSession): Option[Throwable] = {
    val checker = HostnameChecker.getInstance(HostnameChecker.TYPE_TLS)
    val isValid = session.getPeerCertificates.headOption map {
      case x509: X509Certificate =>
        Try { checker.`match`(hostname, x509) } isReturn
      case _ => false
     } getOrElse false

     if (isValid) None else {
       Some(new SslHostVerificationException(session.getPeerPrincipal.getName))
     }
  }
}
