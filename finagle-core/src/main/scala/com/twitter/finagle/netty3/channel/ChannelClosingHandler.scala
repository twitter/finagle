package com.twitter.finagle.netty3.channel

/**
 * A Netty channel handler that reliably closes its underlying
 * connection (when it exists).
 */

import org.jboss.netty.channel.{
  SimpleChannelHandler, LifeCycleAwareChannelHandler,
  ChannelHandlerContext, ChannelStateEvent, Channel, ChannelFutureListener, ChannelFuture}

import com.twitter.finagle.netty3.LatentChannelFuture

private[finagle] class ChannelClosingHandler
  extends SimpleChannelHandler
  with LifeCycleAwareChannelHandler
{
  private[this] val channelCloseFuture = new LatentChannelFuture
  private[this] var channel: Channel = null
  private[this] var awaitingClose = false

  private[this] def setChannel(ch: Channel) = synchronized {
    channel = ch
    channelCloseFuture.setChannel(ch)
    if (awaitingClose) {
      channel.close().addListener(new ChannelFutureListener {
        override def operationComplete(f: ChannelFuture): Unit =
          if (f.isSuccess) {
            channelCloseFuture.setSuccess()
          } else if (f.isCancelled) {
            channelCloseFuture.cancel()
          } else {
            channelCloseFuture.setFailure(f.getCause)
          }
        })
    }
  }

  def close() = synchronized {
    if (channel ne null) {
      channel.close()
    } else {
      awaitingClose = true
      channelCloseFuture
    }
  }

  override def beforeAdd(ctx: ChannelHandlerContext) {
    if (ctx.getPipeline.isAttached)
      setChannel(ctx.getChannel)
  }

  def afterAdd(ctx: ChannelHandlerContext)     {/*nop*/}
  def beforeRemove(ctx: ChannelHandlerContext) {/*nop*/}
  def afterRemove(ctx: ChannelHandlerContext)  {/*nop*/}

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    setChannel(ctx.getChannel)
    super.channelOpen(ctx, e)
  }
}
