package com.twitter.finagle.netty3.channel

import org.jboss.netty.channel._

import com.twitter.util.{Future, Promise, Return}

private[finagle] trait ConnectionLifecycleHandler
  extends SimpleChannelHandler
  with LifeCycleAwareChannelHandler
{
  private[this] def channelDidConnect(ctx: ChannelHandlerContext) {
    val onClose = new Promise[Unit]
    channelConnected(ctx, onClose)
    ctx.getChannel.getCloseFuture.addListener(new ChannelFutureListener {
      override def operationComplete(f: ChannelFuture): Unit =
        if (!f.isCancelled) { // on success or failure
          onClose.update(Return.Unit)
        }
    })
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
