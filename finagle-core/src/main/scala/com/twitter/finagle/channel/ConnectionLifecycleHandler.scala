package com.twitter.finagle.channel

import org.jboss.netty.channel.{
  SimpleChannelHandler, LifeCycleAwareChannelHandler,
  ChannelHandlerContext, ChannelStateEvent}

import com.twitter.util.{Future, Promise, Return}

import com.twitter.finagle.netty3.Conversions._

private[finagle] trait ConnectionLifecycleHandler
  extends SimpleChannelHandler
  with LifeCycleAwareChannelHandler
{
  private[this] def channelDidConnect(ctx: ChannelHandlerContext) {
    val onClose = new Promise[Unit]
    channelConnected(ctx, onClose)
    ctx.getChannel.getCloseFuture() onSuccessOrFailure {
      onClose() = Return(())
    }
  }

  protected def channelConnected(ctx: ChannelHandlerContext, onClose: Future[Unit]): Unit

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channelDidConnect(ctx)
    super.channelOpen(ctx, e)
  }

  override def beforeAdd(ctx: ChannelHandlerContext) {
    if (ctx.getPipeline.isAttached && ctx.getChannel.isOpen)
      channelDidConnect(ctx)
  }

  override def afterAdd(ctx: ChannelHandlerContext)     {/*nop*/}
  override def beforeRemove(ctx: ChannelHandlerContext) {/*nop*/}
  override def afterRemove(ctx: ChannelHandlerContext)  {/*nop*/}
}
