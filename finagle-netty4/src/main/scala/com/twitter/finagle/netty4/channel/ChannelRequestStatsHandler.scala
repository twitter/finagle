package com.twitter.finagle.netty4.channel

import com.twitter.finagle.netty4.channel.ChannelRequestStatsHandler.SharedChannelRequestStats
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import io.netty.channel._

private object ChannelRequestStatsHandler {

  /**
   * Stores all stats that are aggregated across all channels for the client
   * or server.
   */
  class SharedChannelRequestStats(statsReceiver: StatsReceiver) {
    val requestCount = statsReceiver.stat(Verbosity.Debug, "connection_requests")
  }
}

/**
 * A channel stats handler that keeps per-connection request
 * statistics. This handler should be after the request codec in the
 * stack as it assumes messages are POJOs with request/responses.
 *
 * @param sharedChannelRequestStats Aggregates statistics across all channels.
 */
private class ChannelRequestStatsHandler(sharedChannelRequestStats: SharedChannelRequestStats)
    extends ChannelInboundHandlerAdapter {

  private[this] var channelActive: Boolean = false
  private[this] var requestCount: Long = _

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    channelActive = true
    requestCount = 0L
    super.handlerAdded(ctx)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    // Protect against multiple calls
    if (channelActive) {
      sharedChannelRequestStats.requestCount.add(requestCount)
      super.channelInactive(ctx)
    }
    channelActive = false
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    requestCount += 1
    super.channelRead(ctx, msg)
  }
}
