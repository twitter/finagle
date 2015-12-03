package com.twitter.finagle.netty4.channel

import com.twitter.finagle.stats.StatsReceiver
import io.netty.channel._
import io.netty.util.AttributeKey
import java.util.concurrent.atomic.AtomicInteger

private[finagle] object ChannelRequestStatsHandler {
  private[channel] val ConnectionRequestsKey: AttributeKey[AtomicInteger] =
    AttributeKey.valueOf("ChannelRequestStatsHandler.connection_requests")
}

/**
 * A channel stats handler that keeps per-connection request
 * statistics. This handler should be after the request codec in the
 * stack as it assumes messages are POJOs with request/responses.
 *
 * @param statsReceiver the [[StatsReceiver]] to which stats are reported
 */
private[finagle] class ChannelRequestStatsHandler(statsReceiver: StatsReceiver)
  extends ChannelInboundHandlerAdapter
{
  import ChannelRequestStatsHandler.ConnectionRequestsKey

  override def isSharable: Boolean = true

  private[this] val requestCount = statsReceiver.stat("connection_requests")

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.attr(ConnectionRequestsKey).set(new AtomicInteger(0))
    super.channelActive(ctx)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    requestCount.add(ctx.attr(ConnectionRequestsKey).get.get)
    super.channelInactive(ctx)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    val readCount = ctx.attr(ConnectionRequestsKey).get
    readCount.incrementAndGet()
    super.channelRead(ctx, msg)
  }
}
