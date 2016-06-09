package com.twitter.finagle.netty4.proxy

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.channel.{ConnectPromiseDelayListeners, BufferingChannelOutboundHandler}
import io.netty.channel.{Channel, ChannelPromise, ChannelHandlerContext, ChannelOutboundHandlerAdapter}
import io.netty.handler.proxy.Socks5ProxyHandler
import io.netty.util.concurrent.{Future => NettyFuture, GenericFutureListener}
import java.net.SocketAddress

/**
 * An internal handler that upgrades the pipeline to delay connect-promise satisfaction until the
 * SOCKS5 [1] handshake with a proxy server (represented by a `proxyAddress`) succeeds.
 *
 * This handler is layered on top of Netty's [[Socks5ProxyHandler]] that represents a proxy
 * address as a [[SocketAddress]], which will likely be resolved before it's passed into a handler
 * constructor. This means, a single destination to a SOCKS server will be used across a Finagle
 * client, which makes it quite sensitive to proxy server failures. With that said, SOCKS proxy
 * support in Finagle is designed and implemented exclusively for testing/development (assuming that
 * SOCKS proxy is provided via `ssh -D`), not for production usage.
 *
 * For production traffic, an HTTP proxy (see [[HttpProxyConnectHandler]]) should be used instead.
 *
 * [1]: http://www.ietf.org/rfc/rfc1928.txt
 */
private[netty4] class SocksProxyConnectHandler(
    proxyAddress: SocketAddress,
    credentialsOption: Option[Transporter.Credentials])
  extends ChannelOutboundHandlerAdapter
  with BufferingChannelOutboundHandler
  with ConnectPromiseDelayListeners { self =>

  private[this] val socksCodecKey: String = "socks codec"
  private[proxy] var connectPromise: NettyFuture[Channel] = _ // exposed for testing

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    val proxyHandler = credentialsOption match {
      case None => new Socks5ProxyHandler(proxyAddress)
      case Some(c) => new Socks5ProxyHandler(proxyAddress, c.username, c.password)
    }

    connectPromise = proxyHandler.connectFuture()
    ctx.pipeline().addBefore(ctx.name(), socksCodecKey, proxyHandler)
  }

  override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
    ctx.pipeline().remove(socksCodecKey)
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

    // React on satisfied SOCKS handshake promise.
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
          // SOCKS handshake promise is failed so given `Socks5ProxyHandler` is going to close the
          // channel we only  need to fail pending writes and the connect promise.
          promise.tryFailure(future.cause())
          failPendingWrites(ctx, future.cause())
        }
      }
    })

    ctx.connect(remote, local, proxyConnectPromise)
  }

  // We don't override either `exceptionCaught` or `channelInactive` here since `Socks5ProxyHandler`
  // guarantees to fail the connect promise (which we're already handling here) if any exception
  // (caused by either inbound or outbound event or closed channel) occurs during the SOCKS5
  // handshake.
}
