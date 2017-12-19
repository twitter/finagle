package com.twitter.finagle.http2.transport

import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
import io.netty.channel.{
  ChannelInboundHandlerAdapter,
  ChannelOutboundHandlerAdapter,
  ChannelPromise,
  ChannelHandlerContext,
  ChannelHandler
}
import java.net.SocketAddress

/**
 * Buffers until `channelActive` so we can ensure the connection preface is
 * the first message we send.
 */
private[http2] class BufferingHandler extends ChannelInboundHandlerAdapter with BufferingChannelOutboundHandler {
  self =>

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.fireChannelActive()
    // removing a BufferingChannelOutboundHandler writes and flushes too.
    ctx.pipeline.remove(self)
  }

  def bind(ctx: ChannelHandlerContext, addr: SocketAddress, promise: ChannelPromise): Unit = {
    ctx.bind(addr, promise)
  }
  def close(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
    ctx.close(promise)
  }
  def connect(
    ctx: ChannelHandlerContext,
    local: SocketAddress,
    remote: SocketAddress,
    promise: ChannelPromise
  ): Unit = {
    ctx.connect(local, remote, promise)
  }
  def deregister(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
    ctx.deregister(promise)
  }
  def disconnect(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
    ctx.disconnect(promise)
  }
  def read(ctx: ChannelHandlerContext): Unit = {
    ctx.read()
  }
}

private[http2] object BufferingHandler {
  def priorKnowledge(): ChannelHandler = new BufferingHandler()
  def alpn(): ChannelHandler =
    new ChannelOutboundHandlerAdapter with BufferingChannelOutboundHandler

  val HandlerName = "buffer"
}
