package com.twitter.finagle.filter

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import com.twitter.util.{Monitor, Promise, Return, Throw}

import com.twitter.finagle.Service

class MonitorFilterSpec extends SpecificationWithJUnit with Mockito {
  class MockMonitor extends Monitor {
      def handle(cause: Throwable) = false
    }

  "MonitorFilter" should {
    val monitor = spy(new MockMonitor)
    val underlying = mock[Service[Int, Int]]
    val reply = new Promise[Int]
    underlying(any) returns reply
    val service = new MonitorFilter(monitor) andThen underlying
    val exc = new RuntimeException

    "report Future.exception" in {
      val f = service(123)
      f.poll must beNone

      reply() = Throw(exc)
      f.poll must beSome(Throw(exc))
      there was one(monitor).handle(exc)
    }

    "report raw service exception" in {
      underlying(any) throws exc
      val f = service(123)
      f.poll must beSome(Throw(exc))
      there was one(monitor).handle(exc)
    }
  }
}
