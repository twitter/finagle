package com.twitter.finagle.channel

import org.jboss.netty.channel.MessageEvent

trait Endpoint extends Broker {
  def load: Long
}

class LeastLoadedBroker(endpoints: Seq[Endpoint]) extends Broker {
  def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent) {
    leastLoadedEndpoint.dispatch(handlingChannel, e)
  }

  private def leastLoadedEndpoint =
    endpoints.min(new Ordering[Endpoint] {
    def compare(a: Endpoint, b: Endpoint) =
      a.load compare b.load
  })
}