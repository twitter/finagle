package com.twitter.finagle.service

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Promise, Future, Return, Throw, Try}

import com.twitter.finagle.{Service, WriteException}
import com.twitter.finagle.stats.{StatsReceiver, Stat}

object RetryingFilterSpec extends Specification with Mockito {
  "RetryingFilter" should {
    val strategy = mock[RetryStrategy[Int]]
    val strategyPromise = new Promise[Option[RetryStrategy[Int]]]
    strategy.nextStrategy returns strategyPromise
    val stats = mock[StatsReceiver]
    val stat = mock[Stat]
    stats.stat("retries") returns stat
    val shouldRetry = mock[PartialFunction[Try[Int], Boolean]]
    shouldRetry.isDefinedAt(any) returns true
    shouldRetry(any[Try[Int]]) answers {
      case Throw(_:WriteException) => 
        true
      case _ => false
    }
    val filter = new RetryingFilter[Int, Int](strategy, stats, shouldRetry)
    val service = mock[Service[Int, Int]]
    service(123) returns Future(321)
    val retryingService = filter andThen service

    "always try once" in {
      retryingService(123)() must be_==(321)
      there was one(service)(123)
      there was no(strategy).nextStrategy
      there was no(stat).add(any[Int])
    }

    "when failed with a WriteException, consult the retry strategy" in {
      service(123) returns Future.exception(new WriteException(new Exception))
      val f = retryingService(123)
      there was one(service)(123)
      there was one(strategy).nextStrategy
      f.isDefined must beFalse

      // Next time, it succeeds
      service(123) returns Future(321)
      strategyPromise() = Return(Some(strategy))

      there were two(service)(123)
      there was one(stat).add(1)
      f() must be_==(321)
    }

    "give up when the retry strategy is exhausted" in {
      service(123) returns Future.exception(new WriteException(new Exception("i'm exhausted")))
      val f = retryingService(123)
      f.isDefined must beFalse
      there was one(service)(123)
      there was one(strategy).nextStrategy
      there was no(stat).add(any[Int])

      strategyPromise() = Return(None)
      f.isDefined must beTrue
      f.isThrow must beTrue
      f() must throwA(new WriteException(new Exception("i'm exhausted")))
    }

    "when failed with a non-WriteException, fail immediately" in {
      service(123) returns Future.exception(new Exception("WTF!"))
      retryingService(123)() must throwA(new Exception("WTF!"))
      there was one(service)(123)
      there was no(strategy).nextStrategy
      there was no(stat).add(any[Int])
    }

    "when no retry occurs, no stat update" in {
      retryingService(123)() must be_==(321)
      there was no(stat).add(any[Int])
    }
  }

  "NumTriesRetryStrategy" should {
    "immediately yield a new RetryStrategy until it is exhausted" in {
      val strategy = new NumTriesRetryStrategy[Int](2)
      val Some(first) = strategy.nextStrategy()
      val Some(second) = first.nextStrategy()
      second.nextStrategy() must beNone
    }
  }
}
