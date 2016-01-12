package com.twitter.finagle.netty3.channel

import com.twitter.finagle.stats.StatsReceiver
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.channel.{ChannelHandlerContext, ChannelStateEvent, MessageEvent, SimpleChannelHandler}

/**
 * A channel stats handler that keeps per-connection request
 * statistics. This handler should be after the request codec in the
 * stack as it assumes messages are POJOs with request/responses.
 */
private[finagle] class ChannelRequestStatsHandler(statsReceiver: StatsReceiver)
  extends SimpleChannelHandler
{
  private[this] val requestCount = statsReceiver.stat("connection_requests")

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    ctx.setAttachment(new AtomicInteger(0))
    super.channelOpen(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    val attachment = ctx.getAttachment()
    if (attachment != null ) requestCount.add(attachment.asInstanceOf[AtomicInteger].get)

    super.channelClosed(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    val counter = ctx.getAttachment().asInstanceOf[AtomicInteger]
    counter.incrementAndGet()
    super.messageReceived(ctx, e)
  }
}
