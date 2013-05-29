package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Future, Promise, Return, Throw, Time}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ReusingPoolTest extends FunSuite with MockitoSugar {
  class Ctx {
    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])).thenReturn(Future.Done)
    when(underlying.isAvailable).thenReturn(true)
    val service = mock[Service[Int, Int]]
    when(service.close(any[Time])).thenReturn(Future.Done)
    when(service.isAvailable).thenReturn(true)
    val service2 = mock[Service[Int, Int]]
    when(service2.close(any[Time])).thenReturn(Future.Done)
    when(service2.isAvailable).thenReturn(true)
    val underlyingP = new Promise[Service[Int, Int]]
    when(underlying(any[ClientConnection])).thenReturn(underlyingP)
    val pool = new ReusingPool(underlying, NullStatsReceiver)
  }

  test("available when underlying is") {
    val ctx = new Ctx
    import ctx._
    
    assert(pool.isAvailable)
    when(underlying.isAvailable).thenReturn(false)
    assert(!pool.isAvailable)

    verify(underlying, times(2)).isAvailable
  }

  test("first attempt establishes a connection, subsequent attempts don't") {
    val ctx = new Ctx
    import ctx._
     
    verify(underlying, never).apply(any[ClientConnection])
    val f = pool()
    assert(f.poll === None)
    verify(underlying, times(1)).apply(any[ClientConnection])
    val g = pool()
    assert(g.poll === None)
    verify(underlying, times(1)).apply(any[ClientConnection])
    assert(f eq g)
    underlyingP.setValue(service)
    assert(f.poll === Some(Return(service)))
  }

  test("reestablish connections when the service becomes unavailable, releasing dead service") {
    val ctx = new Ctx
    import ctx._

    underlyingP.setValue(service)
    assert(pool().poll === Some(Return(service)))
    verify(underlying, times(1)).apply(any[ClientConnection])
    when(service.isAvailable).thenReturn(false)
    when(underlying(any[ClientConnection])).thenReturn(Future.value(service2))
    verify(service, never).close(any[Time])
    verify(service, never).isAvailable
    assert(pool().poll === Some(Return(service2)))
    verify(service, times(1)).isAvailable
    verify(service, times(1)).close(any[Time])
    verify(underlying, times(2)).apply(any[ClientConnection])
    verify(service2, never).close(any[Time])
  }
  
  test("return on failure") {
    val ctx = new Ctx
    import ctx._

    val exc = new Exception
    underlyingP.setException(exc)
    assert(pool().poll === Some(Throw(exc)))
    verify(underlying, times(1)).apply(any[ClientConnection])
    assert(pool().poll === Some(Throw(exc)))
    verify(underlying, times(2)).apply(any[ClientConnection])
  }
}
