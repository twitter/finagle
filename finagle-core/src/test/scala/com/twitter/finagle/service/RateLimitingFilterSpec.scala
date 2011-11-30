package com.twitter.finagle.service

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.finagle.Service
import com.twitter.util.TimeConversions._
import org.mockito.Matchers
import com.twitter.util.{Time, Future}

object RateLimitingFilterSpec extends Specification with Mockito {

  "RateLimitingFilter" should {
    def categorize(i: Int) = (i%5).toString
    val store = new LocalRateLimitingStore[Int](categorize)
    val filter = new RateLimitingFilter[Int, Int](1.second, 5, store)
    val service = mock[Service[Int, Int]]
    service(Matchers.anyInt) returns Future.value(1)

    val rateLimitedService = filter andThen service

    "Execute requests below rate limit" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        (1 to 5) foreach { _ =>
          rateLimitedService(1)() mustBe 1
          t += 100.milliseconds
        }
      }
    }

    "Refuse request if rate is above limit" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        (1 to 5) foreach { _ =>
          rateLimitedService(1)() mustBe 1
          t += 100.milliseconds
        }

        rateLimitedService(1)() must throwA[Exception]
      }
    }

    "Execute different categories of requests and keep a window per category" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        (1 to 5) foreach { _ =>
          (1 to 5) foreach { i => rateLimitedService(i)() mustBe 1 }
          t += 100.milliseconds
        }
      }
    }

    "Execute different categories of requests and keep a window per category" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        (1 to 5) foreach { _ =>
          (1 to 5) foreach { i => rateLimitedService(i)() mustBe 1 }
          t += 100.milliseconds
        }
      }
    }
  }
}