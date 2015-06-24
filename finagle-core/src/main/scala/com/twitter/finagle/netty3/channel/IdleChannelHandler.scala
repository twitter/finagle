package com.twitter.finagle.netty3.channel

import com.twitter.finagle.stats.StatsReceiver
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.timeout.{IdleState, IdleStateAwareChannelHandler, IdleStateEvent}

/**
 * This handler closes a channel if it receives an IDLE event.
 */
class IdleChannelHandler(receiver: StatsReceiver) extends IdleStateAwareChannelHandler {
  private[this] val statsReceiver = receiver.scope("disconnects")

  override def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent)  {
    val state = e.getState
    if (state == IdleState.READER_IDLE || state == IdleState.WRITER_IDLE) {
      statsReceiver.counter(state.toString).incr()
      e.getChannel.close()
    }

    super.channelIdle(ctx, e)
  }
}
