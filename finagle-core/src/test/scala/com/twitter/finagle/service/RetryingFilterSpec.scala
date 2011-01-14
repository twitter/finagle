package com.twitter.finagle.service

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Promise, Future, Return, Throw}

import com.twitter.finagle.{Service, WriteException}

object RetryingFilterSpec extends Specification with Mockito {
  "RetryingFilter" should {
    val strategy = mock[RetryStrategy]
    val strategyPromise = new Promise[RetryStrategy]
    strategy.nextStrategy returns strategyPromise
    val filter = new RetryingFilter[Int, Int](strategy)
    val service = mock[Service[Int, Int]]
    service(123) returns Future(321)
    val retryingService = filter andThen service

    "always try once" in {
      retryingService(123)() must be_==(321)
      there was one(service)(123)
      there was no(strategy).nextStrategy
    }

    "when failed with a WriteException, consult the retry strategy" in {
      service(123) returns Future.exception(new WriteException(new Exception))
      val f = retryingService(123)
      there was one(service)(123)
      there was one(strategy).nextStrategy
      f.isDefined must beFalse

      // Next time, it succeeds
      service(123) returns Future(321)
      strategyPromise() = Return(strategy)

      there were two(service)(123)
      f() must be_==(321)
    }

    "give up when the retry strategy is exhausted" in {
      service(123) returns Future.exception(new WriteException(new Exception))
      val f = retryingService(123)
      f.isDefined must beFalse
      there was one(service)(123)
      there was one(strategy).nextStrategy

      strategyPromise() = Throw(new Exception("i'm exhausted"))
      f.isDefined must beTrue
      f.isThrow must beTrue
      f() must throwA(new WriteException(new Exception))
    }

    "when failed with a non-WriteException, fail immediately" in {
      service(123) returns Future.exception(new Exception("WTF!"))
      retryingService(123)() must throwA(new Exception("WTF!"))
      there was one(service)(123)
      there was no(strategy).nextStrategy
    }
  }

  "NumTriesRetryStrategy" should {
    "immediately yield a new RetryStrategy until it is exhausted" in {
      val strategy = new NumTriesRetryStrategy(2)
      val first = strategy.nextStrategy()
      val second = first.nextStrategy()
      second.nextStrategy() must throwA(new Exception)
    }
  }
}
