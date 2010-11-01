package com.twitter.finagle.channel

import org.jboss.netty.channel.MessageEvent

trait LoadedBroker[A <: LoadedBroker[A]] extends Broker with Ordered[A]

class RequestCountingBroker(underlying: Broker)
  extends Broker with LoadedBroker[RequestCountingBroker]
{
  private[channel] var dispatchCount = 0

  def dispatch(e: MessageEvent) = {
    dispatchCount += 1
    underlying.dispatch(e)
  }

  def compare(that: RequestCountingBroker) =
    dispatchCount compare that.dispatchCount
}

class LeastLoadedBroker[A <: LoadedBroker[A]](endpoints: Seq[A]) extends Broker {
  def dispatch(e: MessageEvent) =
    leastLoadedEndpoint.dispatch(e)

  private def leastLoadedEndpoint = endpoints.min
}
