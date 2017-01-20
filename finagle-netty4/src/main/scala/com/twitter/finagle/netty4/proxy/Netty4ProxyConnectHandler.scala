package com.twitter.finagle.netty4.proxy

import com.twitter.finagle.netty4.channel.{ConnectPromiseDelayListeners, BufferingChannelOutboundHandler}
import io.netty.channel.{
  Channel, ChannelPromise, ChannelHandlerContext, ChannelOutboundHandlerAdapter
}
import io.netty.handler.proxy.ProxyHandler
import io.netty.util.concurrent.{Future => NettyFuture, GenericFutureListener}
import java.net.SocketAddress

/**
 * An internal handler that upgrades the pipeline to delay connect-promise satisfaction until the
 * handshake (either HTTP or SOCKS5) with a proxy server (represented by a `proxyAddress`) succeeds.
 *
 * This handler is layered on top of Netty's [[ProxyHandler]] that represents a proxy
 * address as a [[SocketAddress]], which will likely be resolved before it's passed into a handler
 * constructor. This means, a single destination to a server will be used across a Finagle
 * client, which makes it quite sensitive to proxy server failures. With that said, this handler
 * is designed and implemented exclusively for testing/development, not for production usage.
 *
 * For production traffic, an HTTP proxy (see [[HttpProxyConnectHandler]]) should be used instead.
 */
private[netty4] class Netty4ProxyConnectHandler(
    proxyHandler: ProxyHandler)
  extends ChannelOutboundHandlerAdapter
  with BufferingChannelOutboundHandler
  with ConnectPromiseDelayListeners { self =>

  private[this] val proxyCodecKey: String = "proxy codec"
  private[proxy] var connectPromise: NettyFuture[Channel] = _ // exposed for testing

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    connectPromise = proxyHandler.connectFuture()
    ctx.pipeline().addBefore(ctx.name(), proxyCodecKey, proxyHandler)
    super.handlerAdded(ctx)
  }

  override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
    ctx.pipeline().remove(proxyCodecKey)
    super.handlerRemoved(ctx)
  }

  override def connect(
    ctx: ChannelHandlerContext,
    remote: SocketAddress,
    local: SocketAddress,
    promise: ChannelPromise
  ): Unit = {
    val proxyConnectPromise = ctx.newPromise()

    // Cancel new promise if an original one is canceled.
    promise.addListener(proxyCancellationsTo(proxyConnectPromise, ctx))

    // Fail the original promise if a new one is failed.
    proxyConnectPromise.addListener(proxyFailuresTo(promise))

    // React on satisfied proxy handshake promise.
    connectPromise.addListener(new GenericFutureListener[NettyFuture[Channel]] {
      override def operationComplete(future: NettyFuture[Channel]): Unit = {
        if (future.isSuccess) {
          // We "try" because it might be already cancelled and we don't need to handle
          // cancellations here - it's already done by `proxyCancellationsTo`.
          // Same thing about `tryFailure` below.
          if (promise.trySuccess()) {
            ctx.pipeline().remove(self) // drains pending writes when removed
          }
        } else {
          // SOCKS/HTTP proxy handshake promise is failed so given `ProxyHandler` is going to
          // close the channel, we only need to fail pending writes and the connect promise.
          promise.tryFailure(future.cause())
          failPendingWrites(ctx, future.cause())
        }
      }
    })

    ctx.connect(remote, local, proxyConnectPromise)
  }

  // We don't override either `exceptionCaught` or `channelInactive` here since `ProxyHandler`
  // guarantees to fail the connect promise (which we're already handling here) if any exception
  // (caused by either inbound or outbound event or closed channel) occurs during the proxy
  // handshake.
}
