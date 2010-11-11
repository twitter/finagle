package com.twitter.finagle.channel

import scala.util.Random

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

import com.twitter.finagle.util.SampleRepository

object LoadedBrokerSpec extends Specification with Mockito {
  class FakeLoadedBroker extends LoadedBroker[FakeLoadedBroker] {
    def load = 0
    def dispatch(e: MessageEvent) = null
  }

  "StatsLoadedBroker" should {
    "increment on dispatch" in {
      val messageEvent = mock[MessageEvent]

      val broker1 = mock[Broker]
      broker1.dispatch(messageEvent) returns ReplyFuture.success("1")
      val broker2 = mock[Broker]
      broker2.dispatch(messageEvent) returns ReplyFuture.success("2")

      val rcBroker1 = new StatsLoadedBroker(broker1, new SampleRepository)
      val rcBroker2 = new StatsLoadedBroker(broker2, new SampleRepository)

      (0 until 3) foreach { _ => rcBroker1.dispatch(messageEvent) }
      (0 until 1) foreach { _ => rcBroker2.dispatch(messageEvent) }

      rcBroker1.load must be_==(3)
      rcBroker2.load must be_==(1)

      (0 until 3) foreach { _ => rcBroker2.dispatch(messageEvent) }

      rcBroker1.load must be_==(3)
      rcBroker2.load must be_==(4)
    }
  }

  "LeastLoadedBroker" should {
    val request = mock[MessageEvent]

    "dispatch to the least loaded" in {
      val loadedBroker1 = spy(new FakeLoadedBroker)
      val loadedBroker2 = spy(new FakeLoadedBroker)

      val leastLoadedBroker = new LeastLoadedBroker(Seq(loadedBroker1, loadedBroker2))

      loadedBroker1.load returns 1
      loadedBroker2.load returns 2

      leastLoadedBroker.dispatch(request)
      there was one(loadedBroker1).dispatch(request)

      loadedBroker1.load returns 3

      leastLoadedBroker.dispatch(request)
      there was one(loadedBroker2).dispatch(request)
    }
  }

  "LoadBalancedBroker" should {
    "dispatch evenly" in {
      val b0 = spy(new FakeLoadedBroker)
      val b1 = spy(new FakeLoadedBroker)
      val theRng = mock[Random]
      val messageEvent = mock[MessageEvent]

      val lb = new LoadBalancedBroker(List(b0, b1)) {
        override val rng = theRng
      }

      theRng.nextInt returns 0
      b0.load returns 1
      b1.load returns 2

      for (i <- 1 to 10) {
        if (i > 5) theRng.nextFloat returns 0.7f
        lb.dispatch(messageEvent)
      }

      there were atLeast(5)(b0).dispatch(messageEvent)
      there were atLeast(5)(b1).dispatch(messageEvent)
    }

    "always picks" in {
      val b0 = spy(new FakeLoadedBroker)
      val lb = new LoadBalancedBroker(List(b0))
      val me = mock[MessageEvent]
      b0.load returns 1
      val f: ReplyFuture = lb.dispatch(me)
      there was one(b0).dispatch(me)
    }

    "when there are no endpoints, returns a failed future" in {
      val lb = new LoadBalancedBroker(List.empty)
      val f: ReplyFuture = lb.dispatch(mock[MessageEvent])
      f.getCause must haveClass[TooFewDicksOnTheDanceFloorException]
    }
  }
}
