package com.twitter.finagle.factory

import com.twitter.conversions.time._
import com.twitter.finagle.{MockTimer, Service, ServiceFactory, ServiceTimeoutException}
import com.twitter.util.{Await, Future, Promise, Return, Time}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class TimeoutFactorySpec extends SpecificationWithJUnit with Mockito {
  "TimeoutFactory" should {
    val timer = new MockTimer
    val underlying = mock[ServiceFactory[String, String]]
    underlying.close(any) returns Future.Done
    val promise = new Promise[Service[String, String]] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler { case exc => interrupted = Some(exc) }
    }
    underlying(any) returns promise
    val timeout = 1.second
    val exception = new ServiceTimeoutException(timeout)
    val factory = new TimeoutFactory(underlying, 1.second, exception, timer)

    "after the timeout" in Time.withCurrentTimeFrozen { tc =>
      val res = factory()
      there was one(underlying)(any)
      promise.interrupted must beNone
      res.isDefined must beFalse
      tc.advance(5.seconds)
      timer.tick()

      "fail the service acquisition" in {
        res.isDefined must beTrue
        Await.result(res) must throwA(exception)
      }

      "interrupt the underlying promise with a TimeoutException" in {
        promise.interrupted must beLike {
          case Some(_: java.util.concurrent.TimeoutException) => true
        }
      }
    }

    "before the timeout" in {
      "pass the successfully created service through" in {
        val res = factory()
        res.isDefined must beFalse
        val service = mock[Service[String, String]]
        service.close(any) returns Future.Done
        promise() = Return(service)
        res.isDefined must beTrue
        Await.result(res) must be_==(service)
      }
    }
  }
}
