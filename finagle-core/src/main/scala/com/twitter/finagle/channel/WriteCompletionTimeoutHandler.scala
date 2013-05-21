package com.twitter.finagle.channel

import com.twitter.finagle.WriteTimedOutException
import com.twitter.finagle.netty3.Conversions._
import com.twitter.util.{Time, Duration, Timer}
import org.jboss.netty.channel.{
  ChannelHandlerContext, Channels, MessageEvent, SimpleChannelDownstreamHandler
}

/**
 * A simple handler that times out a write if it fails to complete
 * within the given time. This can be used to ensure that clients
 * complete reception within a certain time, preventing a resource DoS
 * on a server.
 */
private[finagle] class WriteCompletionTimeoutHandler(timer: Timer, timeout: Duration)
  extends SimpleChannelDownstreamHandler
{
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    val task = timer.schedule(Time.now + timeout) {
      val channel = ctx.getChannel
      Channels.fireExceptionCaught(
        channel, new WriteTimedOutException(if (channel != null) channel.getRemoteAddress else null))
    }
    e.getFuture onSuccessOrFailure { task.cancel() }
    super.writeRequested(ctx, e)
  }
}
