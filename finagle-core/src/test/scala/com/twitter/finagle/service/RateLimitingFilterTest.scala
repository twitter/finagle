package com.twitter.finagle.service

import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class RateLimitingFilterTest extends AnyFunSuite with MockitoSugar {

  class RateLimitingFilterHelper(duration: Duration = 1.second, rate: Int = 5) {
    def categorize(i: Int) = (i % 5).toString

    val strategy = new LocalRateLimitingStrategy[Int](categorize, duration, rate)
    val filter = new RateLimitingFilter[Int, Int](strategy)
    val service = mock[Service[Int, Int]]
    when(service.close(any)) thenReturn Future.Done
    when(service(ArgumentMatchers.anyInt)) thenReturn Future.value(1)

    val rateLimitedService = filter andThen service
  }

  test("RateLimitingFilter should execute requests below rate limit") {
    val h = new RateLimitingFilterHelper
    import h._

    var t = Time.now
    Time.withTimeFunction(t) { _ =>
      (1 to 5) foreach { _ =>
        assert(Await.result(rateLimitedService(1)) == 1)
        t += 100.milliseconds
      }
    }
  }

  test("RateLimitingFilter should refuse one request above rate limit") {
    val h = new RateLimitingFilterHelper
    import h._

    var t = Time.now
    Time.withTimeFunction(t) { _ =>
      (1 to 5) foreach { _ =>
        Await.result(rateLimitedService(1)) == 1
        t += 100.milliseconds
      }

      intercept[Exception] {
        Await.result(rateLimitedService(1))
      }
    }
  }

  test(
    "RateLimitingFilter should execute different categories of requests and keep a window per category") {
    val h = new RateLimitingFilterHelper
    import h._

    var t = Time.now
    Time.withTimeFunction(t) { _ =>
      (1 to 5) foreach { _ =>
        (1 to 5) foreach { i => assert(Await.result(rateLimitedService(i)) == 1) }
        t += 100.milliseconds
      }
    }
  }

  test("RateLimitingFilter should raise exception if rate limit is negative") {
    intercept[IllegalArgumentException] {
      new RateLimitingFilterHelper(rate = 0)
    }
  }

}
