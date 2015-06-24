package com.twitter.finagle.netty3.channel

/**
 * A channel stats handler that keeps per-connection request
 * statistics. This handler should be after the request codec in the
 * stack as it assumes messages are POJOs with request/responses.
 */

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.channel.{
  ChannelHandlerContext, SimpleChannelHandler, MessageEvent}

import com.twitter.util.Future
import com.twitter.finagle.stats.StatsReceiver

private[finagle] class ChannelRequestStatsHandler(statsReceiver: StatsReceiver)
  extends SimpleChannelHandler
  with ConnectionLifecycleHandler
{
  private[this] val requestCount = statsReceiver.stat("connection_requests")

  protected def channelConnected(ctx: ChannelHandlerContext, onClose: Future[Unit]) {
    // The counter:
    ctx.setAttachment(new AtomicInteger(0))

    onClose respond { _ =>
      requestCount.add(ctx.getAttachment().asInstanceOf[AtomicInteger].get)
    }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val counter = ctx.getAttachment().asInstanceOf[AtomicInteger]
    counter.incrementAndGet()
    super.messageReceived(ctx, e)
  }
}
