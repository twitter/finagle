package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

object LeastLoadedBrokerSpec extends Specification with Mockito {
  "LeastLoadedBroker" should {
    val broker1 = mock[LoadedBroker]
    broker1.load returns 1
    val broker2 = mock[LoadedBroker]
    broker2.load returns 2
    val broker3 = mock[LoadedBroker]
    broker3.load returns 3
    val leastLoadedBroker = new LeastLoadedBroker(Seq(broker1, broker2, broker3))
    val channel = mock[BrokeredChannel]
    val messageEvent = mock[MessageEvent]

    "dispatches to the least loaded" in {
      leastLoadedBroker.dispatch(channel, messageEvent)
      there was one(broker1).dispatch(channel, messageEvent)
    }
  }
}