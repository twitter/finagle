package com.twitter.finagle.netty3.channel

import com.twitter.finagle.WriteTimedOutException
import com.twitter.util.{Time, Duration, Timer}
import org.jboss.netty.channel._

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
    e.getFuture.addListener(new ChannelFutureListener {
      override def operationComplete(f: ChannelFuture): Unit =
        if (!f.isCancelled) { // on success or failure
          task.cancel()
        }
    })

    super.writeRequested(ctx, e)
  }
}
