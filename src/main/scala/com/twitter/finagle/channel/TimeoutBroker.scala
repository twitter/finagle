package com.twitter.finagle.channel

import org.jboss.netty.util.{TimerTask, Timeout, Timer}
import org.jboss.netty.channel.MessageEvent

import com.twitter.util.Duration

import com.twitter.finagle.util.Conversions._

class TimeoutBroker(timer: Timer, val underlying: Broker, timeout: Duration)
  extends WrappingBroker
{
  def this(underlying: Broker, timeout: Duration) =
    this(Broker.timer, underlying, timeout)

  override def dispatch(e: MessageEvent) = {
    val future = underlying.dispatch(e)
    val timeoutHandle =
      timer(timeout) {
        future.setFailure(new TimedoutRequestException)
      }

    future whenDone { timeoutHandle.cancel() }
  }
}
