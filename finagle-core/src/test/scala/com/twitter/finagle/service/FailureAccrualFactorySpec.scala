package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import com.twitter.util.{Time, Future}
import com.twitter.conversions.time._

import com.twitter.finagle.{Service, ServiceFactory, MockTimer}

class FailureAccrualFactorySpec extends SpecificationWithJUnit with Mockito {
  "a failing service" should {
    val underlyingService = mock[Service[Int, Int]]
    underlyingService.isAvailable returns true
    underlyingService(Matchers.anyInt) returns Future.exception(new Exception)

    val underlying = mock[ServiceFactory[Int, Int]]
    underlying.isAvailable returns true
    underlying() returns Future.value(underlyingService)

    val timer = new MockTimer
    val factory = new FailureAccrualFactory[Int, Int](
      underlying, 3, 10.seconds, timer)
    val service = factory()()
    there was one(underlying)()

    "become unavailable" in {
      Time.withCurrentTimeFrozen { timeControl =>

        service(123)() must throwA[Exception]
        service(123)() must throwA[Exception]
        factory.isAvailable must beTrue
        service.isAvailable must beTrue

        // Now fail:
        service(123)() must throwA[Exception]
        factory.isAvailable must beFalse
        service.isAvailable must beFalse

        there were three(underlyingService)(123)
      }
    }

    "be revived (for one request) after the markDeadFor duration" in {
      Time.withCurrentTimeFrozen { timeControl =>
        service(123)() must throwA[Exception]
        service(123)() must throwA[Exception]
        service(123)() must throwA[Exception]
        factory.isAvailable must beFalse
        service.isAvailable must beFalse

        timeControl.advance(10.seconds)
        timer.tick()

        // Healthy again!
        factory.isAvailable must beTrue
        service.isAvailable must beTrue

        // But after one bad dispatch, mark it again unhealthy.
        service(123)() must throwA[Exception]

        factory.isAvailable must beFalse
        service.isAvailable must beFalse
      }
    }

    "reset failure counters after an individual success" in {
      Time.withCurrentTimeFrozen { timeControl =>
        service(123)() must throwA[Exception]
        service(123)() must throwA[Exception]
        service(123)() must throwA[Exception]
        factory.isAvailable must beFalse
        service.isAvailable must beFalse

        timeControl.advance(10.seconds)
        timer.tick()

        // Healthy again!
        factory.isAvailable must beTrue
        service.isAvailable must beTrue

        underlyingService(123) returns Future.value(321)

        // A good dispatch!
        service(123)() must be_==(321)

        factory.isAvailable must beTrue
        service.isAvailable must beTrue

        // Counts are now reset.
        underlyingService(123) returns Future.exception(new Exception)
        service(123)() must throwA[Exception]
        factory.isAvailable must beTrue
        service.isAvailable must beTrue
        service(123)() must throwA[Exception]
        factory.isAvailable must beTrue
        service.isAvailable must beTrue
        service(123)() must throwA[Exception]
        factory.isAvailable must beFalse
        service.isAvailable must beFalse
      }
    }
  }

  "a healthy service" should {
    val underlyingService = mock[Service[Int, Int]]
    underlyingService.isAvailable returns true
    underlyingService(Matchers.anyInt) returns Future.value(321)

    val underlying = mock[ServiceFactory[Int, Int]]
    underlying.isAvailable returns true
    underlying() returns Future.value(underlyingService)

    val factory = new FailureAccrualFactory[Int, Int](
      underlying, 3, 10.seconds, new MockTimer)
    val service = factory()()
    there was one(underlying)()

    "[service] pass through underlying availability" in {
      service.isAvailable must beTrue
      underlyingService.isAvailable returns false
      service.isAvailable must beFalse
    }

    "[factory] pass through underlying availability" in {
      factory.isAvailable must beTrue
      service.isAvailable must beTrue
      underlying.isAvailable returns false
      factory.isAvailable must beFalse

      // This propagates to the service as well.
      service.isAvailable must beFalse
    }
  }

  "a broken factory" should {
    val underlying = mock[ServiceFactory[Int, Int]]
    underlying.isAvailable returns true
    val exc = new Exception("i broked :-(")
    underlying() returns Future.exception(exc)
    val factory = new FailureAccrualFactory[Int, Int](
      underlying, 3, 10.seconds, new MockTimer)

    "fail after the given number of tries" in {
      Time.withCurrentTimeFrozen { timeControl =>
        factory.isAvailable must beTrue
        factory()() must throwA(exc)
        factory.isAvailable must beTrue
        factory()() must throwA(exc)
        factory.isAvailable must beTrue
        factory()() must throwA(exc)
        factory.isAvailable must beFalse
      }
    }
  }
}
