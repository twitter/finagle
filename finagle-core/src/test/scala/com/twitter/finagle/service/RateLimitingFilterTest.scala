package com.twitter.finagle.service

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.when
import org.mockito.Matchers
import org.mockito.Matchers._
import com.twitter.util.TimeConversions._
import com.twitter.finagle.Service
import com.twitter.util.{Await, Time, Future}

@RunWith(classOf[JUnitRunner])
class RateLimitingFilterTest extends FunSuite with MockitoSugar {
  class RateLimitingFilterHelper {
    def categorize(i: Int) = (i % 5).toString
    val strategy = new LocalRateLimitingStrategy[Int](categorize, 1.second, 5)
    val filter = new RateLimitingFilter[Int, Int](strategy)
    val service = mock[Service[Int, Int]]
    when(service.close(any)) thenReturn Future.Done
    when(service(Matchers.anyInt)) thenReturn Future.value(1)

    val rateLimitedService = filter andThen service
  }

  test("RateLimitingFilter should Execute requests below rate limit") {
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

  test("RateLimitingFilter should Refuse request if rate is above limit") {
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

  test("RateLimitingFilter should Execute different categories of requests and keep a window per category") {
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
}
