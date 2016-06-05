package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.CancelledConnectionException
import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
import io.netty.channel._
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.{Future => NettyFuture, GenericFutureListener}
import java.net.SocketAddress
import javax.net.ssl.SSLSession

/**
 * Delays the connect promise satisfaction (i.e., `ChannelTransport` creation) until the TLS/SSL
 * handshake is done and [[SSLSession]] validation is succeed (optional).
 *
 * If `sessionValidation` returns `Some` exception, an [[SSLSession]] considered invalid and the
 * connect promise failed with this exception and the channel is closed.
 */
private[netty4] class SslConnectHandler(
    ssl: SslHandler,
    sessionValidation: SSLSession => Option[Throwable]) extends ChannelOutboundHandlerAdapter
  with BufferingChannelOutboundHandler { self =>

  private[this] def fail(p: ChannelPromise, ctx: ChannelHandlerContext, t: Throwable): Unit = {
    p.tryFailure(t)
    failPendingWrites(ctx, t)
  }

  override def connect(
    ctx: ChannelHandlerContext,
    remote: SocketAddress,
    local: SocketAddress,
    promise: ChannelPromise
  ): Unit = {
    val sslConnectPromise = ctx.newPromise()

    // Cancel new promise if the original one is canceled.
    promise.addListener(new GenericFutureListener[NettyFuture[Any]] {
      override def operationComplete(f: NettyFuture[Any]): Unit =
        if (f.isCancelled) {
          if (!sslConnectPromise.cancel(true) && sslConnectPromise.isSuccess) {
            // New connect promise wasn't cancelled because it was already satisfied (connected) so
            // we need to close the channel to prevent resource leaks.
            // See https://github.com/twitter/finagle/issues/345
            failPendingWrites(ctx, new CancelledConnectionException())
            ctx.close()
          }
        }
    })

    // Fail the original promise if a new one is failed.
    sslConnectPromise.addListener(new GenericFutureListener[NettyFuture[Any]] {
      override def operationComplete(f: NettyFuture[Any]): Unit =
        // We filter cancellation here since we assume it was proxied from the original promise and
        // is already being handled in a handler above.
        if (!f.isSuccess && !f.isCancelled) {
          // The connect request was failed so the channel was never active. Since no  writes are
          // expected from the previous handler, no need to fail the pending writes.
          promise.setFailure(f.cause)
        }
    })

    // React on satisfied handshake promise.
    ssl.handshakeFuture().addListener(new GenericFutureListener[NettyFuture[Channel]] {
      override def operationComplete(f: NettyFuture[Channel]): Unit =
        if (f.isSuccess) {
          // Perform session validation.
          sessionValidation(ssl.engine().getSession).foreach { failure =>
            fail(promise, ctx, failure)
            ctx.close()
          }

          // If the original promise is not satisfied yet, then the hostname validation is ether
          // succeed or wasn't performed at all. One way or another, `SslConnectHandler` is done
          // and we don't need that anymore. If the promise already satisfied, the connection was
          // failed and the failure is already propagated to the dispatcher so we don't need to
          // worry about cleaning up the pipeline.
          if (promise.trySuccess()) {
            ctx.pipeline().remove(self) // drains pending writes when removed
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
