package com.twitter.finagle.netty4.proxy

import com.twitter.finagle.ProxyConnectException
import com.twitter.finagle.netty4.channel.ConnectPromiseDelayListeners._
import io.netty.channel.{
  Channel,
  ChannelHandlerContext,
  ChannelOutboundHandlerAdapter,
  ChannelPromise
}
import io.netty.handler.proxy.ProxyHandler
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}
import java.net.{InetSocketAddress, SocketAddress}

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
 *
 * @note This handler doesn't buffer any writes assuming that this is done by [[ProxyHandler]] that
 *       is placed before.
 */
private[netty4] class Netty4ProxyConnectHandler(
  proxyHandler: ProxyHandler,
  bypassLocalhostConnections: Boolean = false)
    extends ChannelOutboundHandlerAdapter { self =>

  private[this] final val proxyCodecKey: String = "netty4ProxyCodec"

  private[this] final def shouldBypassProxy(isa: InetSocketAddress): Boolean =
    bypassLocalhostConnections && !isa.isUnresolved &&
      (isa.getAddress.isLoopbackAddress || isa.getAddress.isLinkLocalAddress)

  private[this] final def connectThroughProxy(
    ctx: ChannelHandlerContext,
    remote: SocketAddress,
    local: SocketAddress,
    promise: ChannelPromise
  ): Unit = {
    // Upgrade the pipeline with the proxy codec pieces.
    ctx.pipeline().addBefore(ctx.name(), proxyCodecKey, proxyHandler)

    val proxyConnectPromise = ctx.newPromise()

    // Cancel new promise if an original one is canceled.
    // NOTE: We don't worry about cancelling/failing pending writes here since it will happen
    // automatically on channel closure.
    promise.addListener(proxyCancellationsTo(proxyConnectPromise, ctx))

    // Fail the original promise if a new one is failed.
    // NOTE: If the connect request fails the channel was never active. Since no
    // writes are expected from the previous handler, no need to fail the pending writes.
    proxyConnectPromise.addListener(proxyFailuresTo(promise))

    // React on satisfied proxy handshake promise.
    proxyHandler.connectFuture.addListener(new GenericFutureListener[NettyFuture[Channel]] {
      override def operationComplete(future: NettyFuture[Channel]): Unit = {
        if (future.isSuccess) {
          ctx.pipeline().remove(proxyCodecKey)
          ctx.pipeline().remove(self)

          promise.trySuccess()
        } else {
          // SOCKS/HTTP proxy handshake promise is failed so given `ProxyHandler` is going to
          // close the channel and fail pending writes, we only need to fail the connect promise.
          promise.tryFailure(
            new ProxyConnectException(future.cause().getMessage, ctx.channel().remoteAddress())
          )
        }
      }
    })

    ctx.connect(remote, local, proxyConnectPromise)
  }

  override def connect(
    ctx: ChannelHandlerContext,
    remote: SocketAddress,
    local: SocketAddress,
    promise: ChannelPromise
  ): Unit = remote match {
    case isa: InetSocketAddress if shouldBypassProxy(isa) =>
      // We're bypassing proxies for any localhost connections.
      ctx.pipeline().remove(self)
      ctx.connect(remote, local, promise)

    case isa: InetSocketAddress if !isa.isUnresolved =>
      // We're replacing resolved InetSocketAddress with unresolved one such that
      // Netty's `HttpProxyHandler` will prefer hostname over the IP address as a destination
      // for a proxy server. This is a safer way to do HTTP proxy handshakes since not
      // all HTTP proxy servers allow for IP addresses to be passed as destinations/host headers.
      val unresolvedRemote = InetSocketAddress.createUnresolved(isa.getHostName, isa.getPort)
      connectThroughProxy(ctx, unresolvedRemote, local, promise)

    case _ =>
      connectThroughProxy(ctx, remote, local, promise)
  }

  // We don't override either `exceptionCaught` or `channelInactive` here since `ProxyHandler`
  // guarantees to fail the connect promise (which we're already handling here) if any exception
  // (caused by either inbound or outbound event or closed channel) occurs during the proxy
  // handshake.
}
