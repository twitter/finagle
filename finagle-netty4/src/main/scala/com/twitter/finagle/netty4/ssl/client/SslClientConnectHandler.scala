package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.{Address, SslVerificationFailedException}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import com.twitter.finagle.netty4.channel.{
  BufferingChannelOutboundHandler,
  ConnectPromiseDelayListeners
}
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
private[netty4] class SslClientConnectHandler(
  sslHandler: SslHandler,
  address: Address,
  config: SslClientConfiguration,
  sessionVerifier: SslClientSessionVerifier
) extends ChannelOutboundHandlerAdapter
    with BufferingChannelOutboundHandler
    with ConnectPromiseDelayListeners { self =>

  private[this] def fail(p: ChannelPromise, ctx: ChannelHandlerContext, t: Throwable): Unit = {
    // We "try" because it might be already cancelled and we don't need to handle
    // cancellations here - it's already done by `proxyCancellationsTo`.
    p.tryFailure(t)
    failPendingWrites(ctx, t)
  }

  private[this] def verifySession(
    ctx: ChannelHandlerContext,
    remote: SocketAddress,
    promise: ChannelPromise,
    session: SSLSession
  ): Unit = {
    try {
      if (!sessionVerifier(address, config, session)) {
        fail(
          promise,
          ctx,
          new SslVerificationFailedException(new Exception("Failed client verification"), remote)
        )
      } // if verification is successful, do nothing here
    } catch {
      case NonFatal(e) =>
        fail(promise, ctx, new SslVerificationFailedException(e, remote))
    }
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

    // React on satisfied handshake promise.
    sslHandler
      .handshakeFuture()
      .addListener(new GenericFutureListener[NettyFuture[Channel]] {
        override def operationComplete(f: NettyFuture[Channel]): Unit =
          if (f.isSuccess) {
            if (sslHandler.engine.isInboundDone) {
              // Likely that the server failed to verify the client or the handshake failed in an unexpected way.
              fail(
                promise,
                ctx,
                new SslVerificationFailedException(
                  new Exception("Failed server verification"),
                  remote
                )
              )
            } else {
              val session = sslHandler.engine().getSession
              verifySession(ctx, remote, promise, session)

              if (promise.trySuccess()) {
                // If the original promise has not yet been satisfied, it can now be marked as successful,
                // and the `SslClientConnectHandler` can be removed from the pipeline. If the promise is
                // already satisfied, the connection was failed and the failure has already been
                // propagated to the dispatcher so we don't need to worry about cleaning up the pipeline.
                ctx.pipeline().remove(self) // drains pending writes when removed
              }
            }
          } else {
            // Handshake promise is failed so given `SslHandler` is going to close the channel we only
            // need to fail pending writes and the connect promise.
            fail(promise, ctx, f.cause())
          }
      })

    // Propagate the connect event down to the pipeline, but use the new (detached) promise.
    ctx.connect(remote, local, sslConnectPromise)
  }

  // We don't override either `exceptionCaught` or `channelInactive` here since `SslHandler`
  // guarantees to fail the handshake promise (which we're already handling here) if any exception
  // (caused by either inbound or outbound event or closed channel) occurs during the handshake.
}
