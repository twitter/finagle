package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.channel.MessageEvent

trait LoadedBroker[A <: LoadedBroker[A]] extends Broker with Ordered[A]

class RequestCountingBroker(underlying: Broker)
  extends Broker with LoadedBroker[RequestCountingBroker]
{
  private[channel] val dispatchCount = new AtomicInteger(0)

  def dispatch(e: MessageEvent) = {
    dispatchCount.incrementAndGet()
    underlying.dispatch(e)
  }

  def compare(that: RequestCountingBroker) =
    dispatchCount.get compare that.dispatchCount.get
}

class LeastLoadedBroker[A <: LoadedBroker[A]](endpoints: Seq[A]) extends Broker {
  def dispatch(e: MessageEvent) =
    leastLoadedEndpoint.dispatch(e)

  private def leastLoadedEndpoint =
    endpoints.min
}
