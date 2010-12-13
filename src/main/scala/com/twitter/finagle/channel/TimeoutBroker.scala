package com.twitter.finagle.channel

import java.util.concurrent.TimeUnit

import org.jboss.netty.util.{TimerTask, Timeout, Timer}
import org.jboss.netty.channel.MessageEvent

import com.twitter.util.Duration

import com.twitter.finagle.util.Conversions._

class TimeoutBroker(timer: Timer, val underlying: Broker, duration: Long, unit: TimeUnit)
  extends WrappingBroker
{
  def this(underlying: Broker, duration: Long, unit: TimeUnit) =
    this(Broker.timer, underlying, duration, unit)

  override def dispatch(e: MessageEvent) = {
    val future = underlying.dispatch(e)
    val timeout =
      timer(Duration.fromTimeUnit(duration, unit)) {
        future.setFailure(new TimedoutRequestException)
      }

    future whenDone { timeout.cancel() }
  }
}
