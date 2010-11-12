package com.twitter.finagle.channel

import java.util.concurrent.TimeUnit

import org.jboss.netty.util.{TimerTask, Timeout, Timer}
import org.jboss.netty.channel.MessageEvent

class TimeoutBroker(timer: Timer, underlying: Broker, duration: Long, unit: TimeUnit)
  extends Broker
{
  def this(underlying: Broker, duration: Long, unit: TimeUnit) =
    this(Broker.timer, underlying, duration, unit)

  def dispatch(e: MessageEvent) = {
    val future = underlying.dispatch(e)
    val timeout = timer.newTimeout(new TimerTask {
      def run(timeout: Timeout) {
        if (!timeout.isCancelled())
          future.setFailure(new TimedoutRequestException)
      }
    }, duration, unit)

    future whenDone { timeout.cancel() }
  }
}
