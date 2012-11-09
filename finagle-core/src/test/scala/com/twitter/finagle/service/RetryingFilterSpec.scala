package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.{MockTimer, Service, WriteException}
import com.twitter.finagle.stats.{StatsReceiver, Stat}
import com.twitter.util._
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class RetryingFilterSpec extends SpecificationWithJUnit with Mockito {
  "RetryingFilter" should {
    val backoffs = Stream(1.second, 2.seconds, 3.seconds)
    val stats = mock[StatsReceiver]
    val retriesStat = mock[Stat]
    val timer = new MockTimer
    stats.stat("retries") returns retriesStat
    val shouldRetry = mock[PartialFunction[Try[Nothing], Boolean]]
    shouldRetry.isDefinedAt(any) returns true
    shouldRetry(any[Try[Nothing]]) answers {
      case Throw(_: WriteException) => true
      case _ => false
    }

    "with RetryPolicy.tries" in {
      val filter = new RetryingFilter[Int, Int](RetryPolicy.tries(3, shouldRetry), timer, stats)
      val service = mock[Service[Int, Int]]
      val retryingService = filter andThen service

      "always try once" in {
        service(123) returns Future(321)
        retryingService(123)() must be_==(321)
        there was one(service)(123)
        there was one(retriesStat).add(0)
      }

      "when failing with WriteExceptions, retry n-1 times" in {
        service(123) returns Future.exception(WriteException(new Exception))
        val f = retryingService(123)
        there were three(service)(123)
        f() must throwA[WriteException]
      }

      "when failed with a non-WriteException, fail immediately" in {
        service(123) returns Future.exception(new Exception("WTF!"))
        retryingService(123)() must throwA(new Exception("WTF!"))
        there was one(service)(123)
        there was one(retriesStat).add(0)
      }

      "when no retry occurs, no stat update" in {
        service(123) returns Future(321)
        retryingService(123)() must be_==(321)
        there was one(retriesStat).add(0)
      }

      "propagate cancellation" in {
        val replyPromise = new Promise[Int]
        service(123) returns replyPromise

        val res = retryingService(123)
        res.isDefined must beFalse
        replyPromise.isCancelled must beFalse

        res.cancel()
        res.isDefined must beFalse
        replyPromise.isCancelled must beTrue
      }
    }

    "with RetryPolicy.backoff" in
      testPolicy(RetryPolicy.backoff(backoffs)(shouldRetry))
    "with RetryPolicy.backoffJava" in
      testPolicy(RetryPolicy.backoffJava(Backoff.toJava(backoffs), shouldRetry))

    def testPolicy(policy: RetryPolicy[Try[Nothing]]) {
      val filter = new RetryingFilter[Int, Int](policy, timer, stats)
      val service = mock[Service[Int, Int]]
      val retryingService = filter andThen service

      "always try once" in {
        service(123) returns Future(321)
        retryingService(123)() must be_==(321)
        there was one(service)(123)
        there was one(retriesStat).add(0)
      }

      "when failed with a WriteException, consult the retry strategy" in Time.withCurrentTimeFrozen { tc =>
        service(123) returns Future.exception(WriteException(new Exception))
        val f = retryingService(123)
        there was one(service)(123)
        f.isDefined must beFalse
        timer.tasks must haveSize(1)

        service(123) returns Future(321)  // we succeed next time; tick!
        tc.advance(1.second); timer.tick()

        there were two(service)(123)
        there was one(retriesStat).add(1)
        f() must be_==(321)
      }

      "give up when the retry strategy is exhausted" in Time.withCurrentTimeFrozen { tc =>
        service(123) returns Future.exception(WriteException(new Exception("i'm exhausted")))
        val f = retryingService(123)
        1 to 3 foreach { i =>
          f.isDefined must beFalse
          there were i.times(service)(123)
          there was no(retriesStat).add(3)
          tc.advance(i.seconds); timer.tick()
        }

        there was one(retriesStat).add(3)
        f.isDefined must beTrue
        f.isThrow must beTrue
        f() must throwA(WriteException(new Exception("i'm exhausted")))
      }

      "when failed with a non-WriteException, fail immediately" in {
        service(123) returns Future.exception(new Exception("WTF!"))
        retryingService(123)() must throwA(new Exception("WTF!"))
        there was one(service)(123)
        timer.tasks must beEmpty
        there was one(retriesStat).add(0)
      }

      "when no retry occurs, no stat update" in {
        service(123) returns Future(321)
        retryingService(123)() must be_==(321)
        there was one(retriesStat).add(0)
      }

      "propagate cancellation" in {
        val replyPromise = new Promise[Int]
        service(123) returns replyPromise

        val res = retryingService(123)
        res.isDefined must beFalse
        replyPromise.isCancelled must beFalse

        res.cancel()
        res.isDefined must beFalse
        replyPromise.isCancelled must beTrue
      }
    }
  }

  "Backoff" should {
    "Backoff.exponential" in {
      val backoffs = Backoff.exponential(1.seconds, 2) take 10
      backoffs.force.toSeq must be_==(0 until 10 map { i => (1 << i).seconds })
    }

    "Backoff.exponential with upper limit" in {
      val backoffs = (Backoff.exponential(1.seconds, 2) take 5) ++ Backoff.const(32.seconds)
      (backoffs take 10).force.toSeq must be_==(0 until 10 map {
          i => (math.min(1 << i, 32)).seconds })
    }

    "Backoff.linear" in {
      val backoffs = Backoff.linear(2.seconds, 10.seconds) take 10
      backoffs.head must be_==(2.seconds)
      backoffs.tail.force.toSeq must be_==(1 until 10 map { i => 2.seconds + 10.seconds * i })
    }

    "Backoff.const" in {
      val backoffs = Backoff.const(10.seconds) take 10
      backoffs.force.toSeq must be_==(0 until 10 map { _ => 10.seconds})
    }
  }
}
