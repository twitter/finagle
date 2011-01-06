package com.twitter.finagle.channel

import org.jboss.netty.util.{TimerTask, Timeout, Timer}
import org.jboss.netty.channel.MessageEvent

import com.twitter.util.{Future, Duration, Promise, Return, Throw}

import com.twitter.finagle.util.TimerFuture
import com.twitter.finagle.util.Conversions._

class TimeoutBroker(timer: Timer, val underlying: Broker, timeout: Duration)
  extends WrappingBroker
{
  def this(underlying: Broker, timeout: Duration) =
    this(Broker.timer, underlying, timeout)

  // TODO: expire the timer?  this shoudl be Future.timeout.
  //
  // Future.timeout(Try[A]) ... (in our rich future library.)
  //
  // YES.. cancel.  constructing the exceptions is expensive.

  override def apply(req: AnyRef) =
    underlying(req).timeout(
      timer, timeout, Throw(new TimedoutRequestException))
}
