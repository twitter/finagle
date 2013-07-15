package com.twitter.finagle.ssl

import com.twitter.finagle.netty3.Conversions._
import com.twitter.finagle.netty3.{Ok, Error, Cancelled}
import com.twitter.finagle.{ChannelClosedException,
  InconsistentStateException, SslHandshakeException, SslHostVerificationException}
import com.twitter.util.Try
import java.net.SocketAddress
import java.security.cert.X509Certificate
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.SSLSession
import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import sun.security.util.HostnameChecker

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

  private[this] def fail(c: Channel, exGen: (SocketAddress) => Throwable) {
    val t = exGen(if (c != null) c.getRemoteAddress else null)
    fail(c, t)
  }

  override def connectRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    e match {
      case de: DownstreamChannelStateEvent =>
        if (!connectFuture.compareAndSet(null, e.getFuture)) {
          fail(ctx.getChannel, new InconsistentStateException(_))
          return
        }

        // proxy cancellation
        val wrappedConnectFuture = Channels.future(de.getChannel, true)
        de.getFuture onCancellation { wrappedConnectFuture.cancel() }
        // Proxy failures here so that if the connect fails, it is
        // propagated to the listener, not just on the channel.
        wrappedConnectFuture.addListener(new ChannelFutureListener {
          def operationComplete(f: ChannelFuture) {
            if (f.isSuccess || f.isCancelled)
              return

            fail(f.getChannel, f.getCause)
          }
        })

        val wrappedEvent = new DownstreamChannelStateEvent(
          de.getChannel, wrappedConnectFuture,
          de.getState, de.getValue)

        super.connectRequested(ctx, wrappedEvent)

      case _ =>
        fail(ctx.getChannel, new InconsistentStateException(_))
    }
  }

  // we delay propagating connection upstream until we've completed the handshake.
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    if (connectFuture.get eq null) {
      fail(ctx.getChannel, new InconsistentStateException(_))
      return
    }

    // proxy cancellations again.
    connectFuture.get.onCancellation {
      fail(ctx.getChannel, new ChannelClosedException(_))
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
        fail(ctx.getChannel, new SslHandshakeException(t, _))

      case Cancelled =>
        fail(ctx.getChannel, new InconsistentStateException(_))
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
