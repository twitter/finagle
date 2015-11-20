package com.twitter.finagle.netty4.channel

import com.twitter.finagle.{NoStacktrace, WriteTimedOutException}
import com.twitter.util.{Duration, Timer}
import io.netty.channel._
import io.netty.util.concurrent.GenericFutureListener

/**
 * A simple handler that times out a write if it fails to complete
 * within the given time. Specifically, the timeout covers the duration
 * before netty has fully flushed the message to the socket.
 *
 * This can be used to ensure that clients complete reception within a
 * certain time, preventing a resource DoS on a server.
 */
private[finagle] class WriteCompletionTimeoutHandler(timer: Timer, timeout: Duration)
  extends ChannelOutboundHandlerAdapter
{
  override def isSharable = true

  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
    val task = timer.doLater(timeout) {
      val writeExn =
        if (ctx.channel != null)
          new WriteTimedOutException(ctx.channel.remoteAddress)
        else
          new WriteTimedOutException

      ctx.fireExceptionCaught(writeExn)
    }

    // cancel task on write completion irrespective of outcome
    promise.addListener(new GenericFutureListener[ChannelPromise] {
      def operationComplete(future: ChannelPromise): Unit =
        task.raise(TimeoutCancelled)
    })

    super.write(ctx, msg, promise)
  }
}

// raised on timeout task after write completes
private[channel] object TimeoutCancelled extends Exception("timeout cancelled") with NoStacktrace
