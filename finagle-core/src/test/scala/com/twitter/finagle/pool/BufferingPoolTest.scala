package com.twitter.finagle.pool

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{times, verify, when}
import org.mockito.Matchers._
import com.twitter.finagle.{ClientConnection, Service, ServiceFactory}
import com.twitter.util.{Await, Future, Time}

class BufferingPoolTest extends FunSuite with MockitoSugar {
  class Helper {
    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    val service = mock[Service[Int, Int]]
    when(service.close(any[Time])) thenReturn Future.Done
    when(service.isAvailable) thenReturn true
    when(underlying(any[ClientConnection])) thenReturn Future.value(service)
    val N = 10
    val pool = new BufferingPool(underlying, N)
  }

  test("BufferingPool should buffer exactly N items") {
    val h = new Helper
    import h._

    val n2 = for (_ <- 0 until N * 2) yield Await.result(pool())
    verify(service, times(0)).close(any[Time])
    verify(underlying, times(N * 2)).apply(any[ClientConnection])
    for (s <- n2 take N)
      s.close()
    verify(service, times(0)).close(any[Time])
    val n1 = for (_ <- 0 until N) yield Await.result(pool())
    verify(underlying, times(N * 2)).apply(any[ClientConnection])
    for (s <- n1)
      s.close()
    verify(service, times(0)).close(any[Time])
    for (s <- n2 drop N)
      s.close()
    verify(service, times(N)).close(any[Time])
  }

  test("BufferingPool should drain services on close") {
    val h = new Helper
    import h._

    val ns = for (_ <- 0 until N) yield Await.result(pool())
    verify(service, times(0)).close(any[Time])
    for (s <- ns take (N - 1)) s.close()
    pool.close()
    verify(service, times(N - 1)).close(any[Time])
    ns(N - 1).close()
    verify(service, times(N)).close(any[Time])

    // Bypass buffer after drained.
    val s = Await.result(pool())
    verify(underlying, times(N + 1)).apply(any[ClientConnection])
    s.close()
    verify(service, times(N + 1)).close(any[Time])
  }

  test("BufferingPool should give back unhealthy services immediately") {
    val h = new Helper
    import h._
    val unhealthy = mock[Service[Int, Int]]
    when(unhealthy.close(any[Time])) thenReturn Future.Done
    when(unhealthy.isAvailable) thenReturn false
    when(underlying(any[ClientConnection])) thenReturn Future.value(unhealthy)
    val s1 = Await.result(pool())
    assert(!s1.isAvailable)
    s1.close()
    verify(unhealthy).close(any[Time])
  }

  test("BufferingPool should skip unhealthy services") {
    val h = new Helper
    import h._

    val failing = mock[Service[Int, Int]]
    when(failing.close(any[Time])) thenReturn Future.Done
    when(failing.isAvailable) thenReturn true
    when(underlying(any[ClientConnection])) thenReturn Future.value(failing)
    Await.result(pool()).close()
    verify(failing, times(0)).close(any[Time])
    when(failing.isAvailable) thenReturn false
    Await.result(pool())
    verify(failing).close(any[Time])
  }
}
