package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

object LeastLoadedBrokerSpec extends Specification with Mockito {
  class FakeLoadedBroker extends LoadedBroker[FakeLoadedBroker] {
    def compare(that: FakeLoadedBroker) = 0
    def dispatch(e: MessageEvent) = null
  }

  "RequestCountingBroker" should {
    "increment on dispatch" in {
      val rcBroker1 = new RequestCountingBroker(mock[Broker])
      val rcBroker2 = new RequestCountingBroker(mock[Broker])

      (0 until 3) foreach { _ => rcBroker1.dispatch(mock[MessageEvent]) }
      (0 until 1) foreach { _ => rcBroker2.dispatch(mock[MessageEvent]) }

      (rcBroker1 compare rcBroker2) mustEqual 1

      (0 until 3) foreach { _ => rcBroker2.dispatch(mock[MessageEvent]) }

      (rcBroker1 compare rcBroker2) mustEqual -1
    }
  }

  "LeastLoadedBroker" should {
    val request = mock[MessageEvent]

    "dispatch to the least loaded" in {
      val loadedBroker1 = spy(new FakeLoadedBroker)
      val loadedBroker2 = spy(new FakeLoadedBroker)

      val leastLoadedBroker = new LeastLoadedBroker(Seq(loadedBroker1, loadedBroker2))

      (loadedBroker1 compare loadedBroker2) returns -1
      leastLoadedBroker.dispatch(request)
      there was one(loadedBroker1).dispatch(request)

      (loadedBroker1 compare loadedBroker2) returns 1
      leastLoadedBroker.dispatch(request)
      there was one(loadedBroker2).dispatch(request)
    }
  }
}
