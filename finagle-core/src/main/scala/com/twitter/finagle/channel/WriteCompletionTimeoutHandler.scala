package com.twitter.finagle.channel

import org.jboss.netty.channel.{
  SimpleChannelDownstreamHandler, Channels,
  ChannelHandlerContext, MessageEvent}

import com.twitter.util.{Time, Duration, Timer}

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.WriteTimedOutException

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
