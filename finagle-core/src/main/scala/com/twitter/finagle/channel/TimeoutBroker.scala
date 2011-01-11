package com.twitter.finagle.channel

import com.twitter.util.{Duration, Throw}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.Timer

class TimeoutBroker(val underlying: Broker, timeout: Duration, timer: Timer = Timer.default)
  extends WrappingBroker
{
  override def apply(req: AnyRef) =
    underlying(req).timeout(
      timer, timeout, Throw(new TimedoutRequestException))
}
