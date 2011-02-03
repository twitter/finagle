package com.twitter.finagle.channel

/**
 * A Netty channel handler that reliably closes its underlying
 * connection (when it exists).
 */

import org.jboss.netty.channel.{
  SimpleChannelUpstreamHandler, LifeCycleAwareChannelHandler,
  ChannelHandlerContext, ChannelStateEvent, Channel}

class ChannelClosingHandler
  extends SimpleChannelUpstreamHandler
  with LifeCycleAwareChannelHandler
{
  private[this] var channel: Channel = null
  private[this] var awaitingClose = false

  private[this] def setChannel(ch: Channel) = synchronized {
    channel = ch
    if (awaitingClose)
      channel.close()
  }

  def close() = synchronized {
    if (channel ne null)
      channel.close()
    else
      awaitingClose = true
  }

  override def beforeAdd(ctx: ChannelHandlerContext) {
    if (!ctx.getPipeline.isAttached)
      return

    setChannel(ctx.getChannel)
  }

  def afterAdd(ctx: ChannelHandlerContext)     {/*nop*/}
  def beforeRemove(ctx: ChannelHandlerContext) {/*nop*/}
  def afterRemove(ctx: ChannelHandlerContext)  {/*nop*/}

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    setChannel(ctx.getChannel)
  }
}
