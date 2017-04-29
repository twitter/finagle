package com.twitter.finagle.netty4.ssl.server

import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}


/**
  * Delays `channelRegistered` event until the TLS handshake is successfully finished.
  */
private[netty4] class SslServerConnectHandler(ssl: SslHandler) extends ChannelInboundHandlerAdapter { self =>

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val channel = ctx.channel()
    ssl.handshakeFuture().addListener(new GenericFutureListener[NettyFuture[Channel]] {
      override def operationComplete(future: NettyFuture[Channel]): Unit = {
        if (future.isSuccess) {
          ctx.pipeline().remove(self)
          ctx.fireChannelActive()
        }
      }
    })
    if (!channel.config().isAutoRead) {
      channel.read()
    }
  }

}
