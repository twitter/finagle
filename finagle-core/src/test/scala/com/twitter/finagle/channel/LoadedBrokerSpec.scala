package com.twitter.finagle.channel

import scala.util.Random

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

import com.twitter.util.{Future, Promise, Return, Throw}
import com.twitter.finagle.util._
import com.twitter.finagle.stats.{ReadableCounter, StatsRepository, SimpleStatsRepository}

object LoadedBrokerSpec extends Specification with Mockito {
  class FakeBroker(reply: Future[AnyRef]) extends Broker {
    def apply(request: AnyRef) = reply
  }

  class FakeLoadedBroker extends LoadedBroker[FakeLoadedBroker] {
    def load = 0
    def apply(request: AnyRef) = null
  }

  val statsRepository = new SimpleStatsRepository

  "StatsLoadedBroker" should {
    "increment on dispatch" in {
      val messageEvent = mock[Object]

      val broker1 = new FakeBroker(Future("1"))
      val broker2 = new FakeBroker(Future("2"))

      val rcBroker1 = new StatsLoadedBroker(broker1, statsRepository)
      val rcBroker2 = new StatsLoadedBroker(broker2, statsRepository)

      (0 until 3) foreach { _ => rcBroker1(messageEvent) }
      (0 until 1) foreach { _ => rcBroker2(messageEvent) }

      rcBroker1.load must be_==(3)
      rcBroker2.load must be_==(1)

      (0 until 3) foreach { _ => rcBroker2(messageEvent) }

      rcBroker1.load must be_==(3)
      rcBroker2.load must be_==(4)
    }

    "be unavailable when the underlying broker is unavailable" in {
      val broker = mock[Broker]
      broker.isAvailable returns false
      val loadedBroker = new StatsLoadedBroker(broker, statsRepository)
      loadedBroker.isAvailable must beFalse
    }

    "have weight = 0.0 when the underlying broker is unavailable" in {
      val broker = mock[Broker]
      broker.isAvailable returns false
      val loadedBroker = new StatsLoadedBroker(broker, statsRepository)
      loadedBroker.weight must be_==(0.0)
    }
  }

  "FailureAccruingLoadedBroker" should {
    class FakeLoadedBroker extends LoadedBroker[FakeLoadedBroker] {
      var theWeight = 0.0f
      var theLoad = 0
      var future: Future[AnyRef] = null

      def apply(request: AnyRef) = future

      def load = theLoad
      override def weight = theWeight
    }

    val underlying = new FakeLoadedBroker

    val statsRepository = mock[StatsRepository]
    val broker = new FailureAccruingLoadedBroker(underlying, statsRepository)

    val successSample = mock[ReadableCounter]
    val failureSample = mock[ReadableCounter]

    statsRepository.counter("success" -> "broker") returns successSample
    statsRepository.counter("failure" -> "broker") returns failureSample

    "account for success" in {
      val e = mock[Object]
      val f = new Promise[Object]

      underlying.future = f
      broker(e) must be_==(f)


      there was no(successSample).incr()
      there was no(failureSample).incr()

      f() = Return("yipee!")
      there was one(successSample).incr()
    }

    "account for failure" in {
      val e = mock[Object]
      val f = new Promise[String]

      underlying.future = f
      broker(e) must be_==(f)

      f() = Throw(new Exception("doh."))

      there was no(successSample).incr()
      there was one(failureSample).incr()
    }

    "take into account failures" in {
      val e = mock[Object]
      val f = new Promise[String]

      underlying.future = f
      broker(e) must be_==(f)

      f() = Return("yipee!")
      there was no(failureSample).incr()
      there was one(successSample).incr()
    }

    "not modify weights for a healhty client" in {
      successSample.sum returns 1
      failureSample.sum returns 0
      underlying.theWeight = 1.0f
      broker.weight must be_==(1.0f)
    }

    "adjust weights for an unhealthy broker" in {
      successSample.sum returns 1
      failureSample.sum returns 1
      underlying.theWeight = 1.0f
      broker.weight must be_==(0.5f)
    }

    "become unavailable when the success rate is 0" in {
      successSample.sum returns 0
      failureSample.sum returns 1
      broker.isAvailable must beFalse
    }
  }

  "LeastLoadedBroker" should {
    val request = mock[Object]

    "dispatch to the least loaded" in {
      val loadedBroker1 = spy(new FakeLoadedBroker)
      val loadedBroker2 = spy(new FakeLoadedBroker)

      val leastLoadedBroker = new LeastLoadedBroker(Seq(loadedBroker1, loadedBroker2))

      loadedBroker1.load returns 1
      loadedBroker2.load returns 2

      leastLoadedBroker(request)
      there was one(loadedBroker1)(request)

      loadedBroker1.load returns 3

      leastLoadedBroker(request)
      there was one(loadedBroker2)(request)
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
      val f = b(mock[Object])
      f.isThrow must beTrue
      f() must throwA(new NoBrokersAvailableException)
    }
  }

  "LoadBalancedBroker" should {
    "dispatch evenly" in {
      val b0 = spy(new FakeLoadedBroker)
      val b1 = spy(new FakeLoadedBroker)
      val theRng = mock[Random]
      val messageEvent = mock[Object]

      val lb = new LoadBalancedBroker(List(b0, b1)) {
        override val rng = theRng
      }

      theRng.nextInt returns 0
      b0.load returns 1
      b1.load returns 2

      for (i <- 1 to 10) {
        if (i > 5) theRng.nextFloat returns 0.7f
        lb(messageEvent)
      }

      there were atLeast(5)(b0)(messageEvent)
      there were atLeast(5)(b1)(messageEvent)
    }

    "always picks" in {
      val b0 = spy(new FakeLoadedBroker)
      val lb = new LoadBalancedBroker(List(b0))
      val me = mock[Object]
      b0.load returns 1
      val f = lb(me)
      there was one(b0)(me)
    }

    "when there are no endpoints, is not available and returns a failed future" in {
      val lb = new LoadBalancedBroker(List.empty)
      val f = lb(mock[Object])
      lb.isAvailable must beFalse
      f() must throwA(new NoBrokersAvailableException)
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
      val f = b(mock[Object])
      f() must throwA(new NoBrokersAvailableException)
    }
  }
}
