package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.{Address, SslVerificationFailedException}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
import com.twitter.finagle.netty4.channel.ConnectPromiseDelayListeners._
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Return, Throw}
import io.netty.channel._
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}
import java.net.SocketAddress
import javax.net.ssl.SSLSession
import scala.util.control.NonFatal

/**
 * Delays the connect promise satisfaction (i.e., `ChannelTransport` creation) until the SSL/TLS
 * handshake is done and session verification has succeeded.
 *
 * If an `SSLSession` fails verification, the SSL/TLS connection is considered invalid and the
 * connect promise failed with an exception and the channel is closed.
 */
private[netty4] class SslClientVerificationHandler(
  sslHandler: SslHandler,
  address: Address,
  config: SslClientConfiguration,
  sessionVerifier: SslClientSessionVerifier)
    extends ChannelOutboundHandlerAdapter
    with BufferingChannelOutboundHandler { self =>

  import SslClientVerificationHandler._

  private[this] val onHandshakeComplete = Promise[Unit]()
  private[this] val inet: Option[SocketAddress] = address match {
    case Address.Inet(addr, _) => Some(addr)
    case _ => None
  }

  val handshakeComplete: Future[Unit] = onHandshakeComplete

  private[this] def fail(t: Throwable): Unit = {
    onHandshakeComplete.setException(t)
    failPendingWrites(t)
  }

  private[this] def verifySession(session: SSLSession, ctx: ChannelHandlerContext): Unit = {
    try {
      if (!sessionVerifier(address, config, session)) {
        fail(
          new SslVerificationFailedException(
            Some(new Exception("Failed client verification")),
            inet
          )
        )
        ctx.close()
      } // if verification is successful, do nothing here
    } catch {
      case NonFatal(e) =>
        fail(new SslVerificationFailedException(Some(e), inet))
    }
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    // React on satisfied handshake promise.
    sslHandler
      .handshakeFuture()
      .addListener(new GenericFutureListener[NettyFuture[Channel]] {
        def operationComplete(f: NettyFuture[Channel]): Unit =
          if (f.isSuccess) {
            if (sslHandler.engine.isInboundDone) {
              // Likely that the server failed to verify the client or the handshake failed in an unexpected way.
              fail(
                new SslVerificationFailedException(
                  Some(new Exception("Failed server verification")),
                  inet
                )
              )
            } else {
              val session = sslHandler.engine().getSession
              verifySession(session, ctx)

              if (onHandshakeComplete.setDone()) {
                // If the original promise has not yet been satisfied, it can now be marked as successful,
                // and the `SslClientVerificationHandler` can be removed from the pipeline. If the promise is
                // already satisfied, the connection was failed and the failure has already been
                // propagated to the dispatcher so we don't need to worry about cleaning up the pipeline.
                ctx.pipeline.remove(self) // drains pending writes when removed
              }
            }
          } else {
            // Handshake promise is failed so given `SslHandler` is going to close the channel we only
            // need to fail pending writes and the connect promise.
            fail(f.cause())
          }
      })

    super.handlerAdded(ctx)
  }

  override def connect(
    ctx: ChannelHandlerContext,
    remote: SocketAddress,
    local: SocketAddress,
    promise: ChannelPromise
  ): Unit = {
    val sslConnectPromise = ctx.newPromise()

    // Cancel new promise if the original one is canceled.
    // NOTE: We don't worry about cancelling/failing pending writes here since it will happen
    // automatically on channel closure.
    promise.addListener(proxyCancellationsTo(sslConnectPromise, ctx))

    // Fail the original promise if a new one is failed.
    // NOTE: If the connect request fails the channel was never active. Since no
    // writes are expected from the previous handler, no need to fail the pending writes.
    sslConnectPromise.addListener(proxyFailuresTo(promise))

    onHandshakeComplete.respond {
      case Return(_) =>
        // Using `trySuccess` here instead of `setSuccess` prevents the exception from
        // going straight to the root monitor when the 'promise' has already been
        // satisfied.
        if (!promise.trySuccess()) {
          // From the standpoint of the `SslClientVerificationHandler`, this `SSLSession`
          // has been established successfully. However, we've seen that it's possible for
          // this 'promise' to have already been failed with a cancellation and still reach
          // this code. If that's the case, we log to further understand why. We have no
          // reason to believe that the 'satisfy an already successful promise' condition
          // will ever be hit, but it is there to be defensive.
          if (promise.isSuccess)
            log.error(
              s"The SslClientVerificationHandler attempted to satisfy an already successful promise: $promise")
          else
            log.debug(
              promise.cause,
              "The SslClientVerificationHandler attempted to satisfy an already failed promise")
        }
      case Throw(exn) =>
        // this is racy because we proxy failures
        promise.tryFailure(exn)
    }

    // Propagate the connect event down to the pipeline, but use the new (detached) promise.
    ctx.connect(remote, local, sslConnectPromise)
  }

  // We don't override either `exceptionCaught` or `channelInactive` here since `SslHandler`
  // guarantees to fail the handshake promise (which we're already handling here) if any exception
  // (caused by either inbound or outbound event or closed channel) occurs during the handshake.
}

object SslClientVerificationHandler {
  private val log = Logger.get(this.getClass)
}
