package com.twitter.finagle.channel

import org.jboss.netty.util.Timer
import com.twitter.util.{Duration, Throw}

import com.twitter.finagle.util.Conversions._

class TimeoutBroker(timer: Timer, val underlying: Broker, timeout: Duration)
  extends WrappingBroker
{
  def this(underlying: Broker, timeout: Duration) =
    this(Broker.timer, underlying, timeout)

  override def apply(req: AnyRef) =
    underlying(req).timeout(
      timer, timeout, Throw(new TimedoutRequestException))
}
