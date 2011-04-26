package com.twitter.finagle.channel

import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.timeout.{IdleState, IdleStateAwareChannelHandler, IdleStateEvent}

/**
 * This handler closes a channel if it receives an IDLE event.
 */
class IdleChannelHandler extends IdleStateAwareChannelHandler {
  override def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent)  {
    if (e.getState() == IdleState.READER_IDLE || e.getState() == IdleState.WRITER_IDLE) {
      e.getChannel.close()
    }
  }
}
