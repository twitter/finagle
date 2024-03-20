package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps.RichDuration
import com.twitter.finagle.CoreToggles
import com.twitter.finagle.Service
import com.twitter.finagle.SimpleFilter
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ToggleAwareSimpleFilterTest extends AnyFunSuite with MockitoSugar {

  def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 5.seconds)

  trait Fixture {
    val underlyingFilter = new SimpleFilter[Long, String] {
      override def apply(
        request: Long,
        service: Service[Long, String]
      ): Future[String] = Future.value("underlying filter")
    }

    val service = new Service[Long, String] {
      override def apply(request: Long): Future[String] = Future.value("service")
    }

    val toggleKey = "com.twitter.finagle.filter.TestToggleAwareUnderlying"
    val toggle = CoreToggles(toggleKey)
    val filter = new ToggleAwareSimpleFilter[Long, String](underlyingFilter, toggle)

  }

  test("calls underlying filter when toggle is enabled") {
    new Fixture {

      com.twitter.finagle.toggle.flag.overrides.let(toggleKey, 1) {
        val result = filter.apply(0L, service)
        assert(await(result) == "underlying filter")
      }
    }
  }

  test("calls actual service when toggle is disabled") {
    new Fixture {

      com.twitter.finagle.toggle.flag.overrides.let(toggleKey, 0) {
        val result = filter.apply(0L, service)
        assert(await(result) == "service")
      }
    }
  }
}
