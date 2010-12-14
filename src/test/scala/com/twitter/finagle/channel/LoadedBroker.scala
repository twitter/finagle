package com.twitter.finagle.channel

import scala.util.Random

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

import com.twitter.finagle.util._

object LoadedBrokerSpec extends Specification with Mockito {
  class FakeLoadedBroker extends LoadedBroker[FakeLoadedBroker] {
    def load = 0
    def dispatch(e: MessageEvent) = null
  }

  class Repo extends LazilyCreatingSampleRepository[ScalarSample] {
    def makeStat = new ScalarSample
  }

  "StatsLoadedBroker" should {
    "increment on dispatch" in {
      val messageEvent = mock[MessageEvent]

      val broker1 = mock[Broker]
      broker1.dispatch(messageEvent) returns ReplyFuture.success("1")
      val broker2 = mock[Broker]
      broker2.dispatch(messageEvent) returns ReplyFuture.success("2")
      
      val rcBroker1 = new StatsLoadedBroker(broker1, new Repo)
      val rcBroker2 = new StatsLoadedBroker(broker2, new Repo)

      (0 until 3) foreach { _ => rcBroker1.dispatch(messageEvent) }
      (0 until 1) foreach { _ => rcBroker2.dispatch(messageEvent) }

      rcBroker1.load must be_==(3)
      rcBroker2.load must be_==(1)

      (0 until 3) foreach { _ => rcBroker2.dispatch(messageEvent) }

      rcBroker1.load must be_==(3)
      rcBroker2.load must be_==(4)
    }

    "be unavailable when the underlying broker is unavailable" in {
      val broker = mock[Broker]
      broker.isAvailable returns false
      val loadedBroker = new StatsLoadedBroker(broker, new Repo)
      loadedBroker.isAvailable must beFalse
    }

    "have weight = 0.0 when the underlying broker is unavailable" in {
      val broker = mock[Broker]
      broker.isAvailable returns false
      val loadedBroker = new StatsLoadedBroker(broker, new Repo)
      loadedBroker.isSuperAvailable must beFalse
      loadedBroker.weight must be_==(0.0)
    }
  }

  "FailureAccruingLoadedBroker" should {
    val underlying = mock[LoadedBroker[T forSome { type  T <: LoadedBroker[T] }]]

    type S = TimeWindowedSample[ScalarSample]

    val repo = mock[SampleRepository[S]]
    val successSample = mock[S]
    val failureSample = mock[S]

    repo("success") returns successSample
    repo("failure") returns failureSample

    val broker = new FailureAccruingLoadedBroker(underlying, repo)

    "account for success" in {
      val e = mock[MessageEvent]
      val f = new ReplyFuture

      underlying.dispatch(e) returns f
      broker.dispatch(e) must be_==(f)

      there was no(successSample).incr()
      there was no(failureSample).incr()

      f.setReply(Reply.Done("yipee!"))
      there was one(successSample).incr()
    }

    "account for failure" in {
      val e = mock[MessageEvent]
      val f = new ReplyFuture

      underlying.dispatch(e) returns f
      broker.dispatch(e) must be_==(f)

      f.setFailure(new Exception("doh."))

      there was no(successSample).incr()
      there was one(failureSample).incr()
    }

    "take into account failures" in {
      val e = mock[MessageEvent]
      val f = new ReplyFuture

      underlying.dispatch(e) returns f
      broker.dispatch(e) must be_==(f)

      f.setReply(Reply.Done("yipee!"))
      there was no(failureSample).incr()
      there was one(successSample).incr()
    }

    "not modify weights for a healhty client" in {
      successSample.count returns 1
      failureSample.count returns 0
      underlying.weight returns 1.0f
      broker.weight must be_==(1.0f)
    }

    "adjust weights for an unhealthy broker" in {
      successSample.count returns 1
      failureSample.count returns 1
      underlying.weight returns 1.0f
      broker.weight must be_==(0.5f)
    }

    "become unavailable when the success rate is 0" in {
      successSample.count returns 0
      failureSample.count returns 1
      broker.isAvailable must beFalse      
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


    "fails when unavailable" in {
      val b1 = spy(new FakeLoadedBroker)
      val b2 = spy(new FakeLoadedBroker)

      b1.load returns 0
      b1.weight returns 0.0f

      b2.load returns 0
      b2.weight returns 0.0f

      b1.isAvailable must beFalse
      b2.isAvailable must beFalse

      val b = new LeastLoadedBroker(Seq(b1, b2))
      val f = b.dispatch(mock[MessageEvent])
      f.isSuccess must beFalse
      f.getCause must haveClass[NoBrokersAvailableException]
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

    "when there are no endpoints, is not available and returns a failed future" in {
      val lb = new LoadBalancedBroker(List.empty)
      val f: ReplyFuture = lb.dispatch(mock[MessageEvent])
      lb.isAvailable must beFalse
      f.getCause must haveClass[NoBrokersAvailableException]
    }

    "fails when unavailable" in {
      val b1 = spy(new FakeLoadedBroker)
      val b2 = spy(new FakeLoadedBroker)

      b1.load returns 0
      b1.weight returns 0.0f

      b2.load returns 0
      b2.weight returns 0.0f

      b1.isAvailable must beFalse
      b2.isAvailable must beFalse

      val b = new LoadBalancedBroker(Seq(b1, b2))
      val f = b.dispatch(mock[MessageEvent])
      f.isSuccess must beFalse
      f.getCause must haveClass[NoBrokersAvailableException]
    }
  }
}
