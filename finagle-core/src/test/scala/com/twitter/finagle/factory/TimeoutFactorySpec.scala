package com.twitter.finagle.factory

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Time, Promise, Return}
import com.twitter.conversions.time._
import com.twitter.finagle.MockTimer
import com.twitter.finagle.{Service, ServiceFactory, ServiceTimeoutException}

object TimeoutFactorySpec extends Specification with Mockito {
  "TimeoutFactory" should {
    val timer = new MockTimer
    val underlying = mock[ServiceFactory[String, String]]
    val promise = new Promise[Service[String, String]]
    underlying.make() returns promise
    val timeout = 1.second
    val exception = new ServiceTimeoutException(timeout)
    val factory = new TimeoutFactory(underlying, 1.second, exception, timer)

    "after the timeout" in Time.withCurrentTimeFrozen { tc =>
      val res = factory.make()
      there was one(underlying).make()
      promise.isCancelled must beFalse
      res.isDefined must beFalse
      tc.advance(5.seconds)
      timer.tick()

      "fail the service acquisition" in {
        res.isDefined must beTrue
        res() must throwA(exception)
      }

      "cancel the underlying promise" in {
        promise.isCancelled must beTrue
      }
    }

    "before the timeout" in {
      "pass the successfully created service through" in {
        val res = factory.make()
        res.isDefined must beFalse
        val service = mock[Service[String, String]]
        promise() = Return(service)
        res.isDefined must beTrue
        res() must be_==(service)
      }
    }
  }
}
