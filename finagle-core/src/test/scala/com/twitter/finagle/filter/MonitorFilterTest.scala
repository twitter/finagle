package com.twitter.finagle.filter

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.times
import org.mockito.Mockito
import com.twitter.finagle.Service

@RunWith(classOf[JUnitRunner])
class MonitorFilterTest extends FunSuite with MockitoSugar {
/*
  class MockMonitor extends Monitor {
    def handle(cause: Throwable) = false
  }

  class MonitorFilterHelper {
    val monitor = spy(new MockMonitor)
    val underlying = mock[Service[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    val reply = new Promise[Int]
    when(underlying(any)) thenReturn reply
    val service = new MonitorFilter(monitor) andThen underlying
    val exc = new RuntimeException
  }

  test("report Future.exception") {
    pending
    val h = new MonitorFilterHelper
    import h._

    val f = service(123)
    f.poll must beNone

    reply() = Throw(exc)
    assert(f.poll == Some(Throw(exc)))
    verify(monitor).handle(exc)
  }

  test("report raw service exception") {
    val h = new MonitorFilterHelper
    import h._

    underlying(any) throws exc
    val f = service(123)
    assert(f.poll == Some(Throw(exc)))
    verify(monitor).handle(exc)
  }
  */

}
