package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Await, Future, Promise, Return, Throw, Time}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SingletonPoolTest extends FunSuite with MockitoSugar {
  class Ctx {
    val underlying = mock[ServiceFactory[Int, Int]]
    val closeP = new Promise[Unit]
    when(underlying.close(any[Time])).thenReturn(closeP)
    when(underlying.status).thenReturn(Status.Open)
    val service = mock[Service[Int, Int]]
    when(service.close(any[Time])).thenReturn(Future.Done)
    when(service.status).thenReturn(Status.Open)
    val service2 = mock[Service[Int, Int]]
    when(service2.close(any[Time])).thenReturn(Future.Done)
    when(service2.status).thenReturn(Status.Open)
    val underlyingP = new Promise[Service[Int, Int]]
    when(underlying(any[ClientConnection])).thenReturn(underlyingP)
    val pool = new SingletonPool(underlying, NullStatsReceiver)

    def assertClosed() {
      val Some(Throw(Failure(Some(cause)))) = pool().poll
      assert(cause.isInstanceOf[ServiceClosedException])
    }
  }

  test("available when underlying is; unavailable when closed") {
    val ctx = new Ctx
    import ctx._

    assert(pool.isAvailable)
    when(underlying.status).thenReturn(Status.Closed)
    assert(!pool.isAvailable)

    verify(underlying, times(2)).status

    when(underlying.status).thenReturn(Status.Open)
    assert(pool.isAvailable)
    pool.close()
    assert(!pool.isAvailable)
  }

  test("underlying availability takes priority") {
    val ctx = new Ctx
    import ctx._

    assert(pool.isAvailable)
    pool()
    assert(pool.isAvailable)
    underlyingP.setValue(service)
    assert(pool.isAvailable)
    when(underlying.status).thenReturn(Status.Closed)
    assert(!pool.isAvailable)
  }

  test("first attempt establishes a connection, subsequent attempts don't") {
    val ctx = new Ctx
    import ctx._

    verify(underlying, never).apply(any[ClientConnection])
    val f = pool()
    assert(f.poll == None)
    verify(underlying, times(1)).apply(any[ClientConnection])
    val g = pool()
    assert(g.poll == None)
    verify(underlying, times(1)).apply(any[ClientConnection])
    underlyingP.setValue(service)
    assert(f.poll == Some(Return(service)))
    assert(g.poll == Some(Return(service)))
  }

  test("reestablish connections when the service becomes unavailable, releasing dead service") {
    val ctx = new Ctx
    import ctx._

    underlyingP.setValue(service)
    val fst = pool()
    assert(fst.poll == Some(Return(service)))
    verify(underlying, times(1)).apply(any[ClientConnection])

    when(service.status).thenReturn(Status.Closed)
    when(underlying(any[ClientConnection])).thenReturn(Future.value(service2))
    verify(service, never).close(any[Time])
    verify(service, times(2)).status

    val snd = pool()
    assert(snd.poll == Some(Return(service2)))
    verify(service, times(3)).status

    verify(service, never).close(any[Time])
    Await.result(fst).close()
    verify(service, times(1)).close(any[Time])

    verify(underlying, times(2)).apply(any[ClientConnection])
    Await.result(snd).close()
    verify(service2, never).close(any[Time])
  }

  test("return on failure") {
    val ctx = new Ctx
    import ctx._

    val exc = new Exception
    underlyingP.setException(exc)
    assert(pool().poll == Some(Throw(exc)))
    verify(underlying, times(1)).apply(any[ClientConnection])
    assert(pool().poll == Some(Throw(exc)))
    verify(underlying, times(2)).apply(any[ClientConnection])
  }

  test("fail when connected service is unavailable") {
    val ctx = new Ctx
    import ctx._

    when(service.status).thenReturn(Status.Closed)
    val f = pool()
    assert(!f.isDefined)
    verify(service, never).close(any[Time])
    underlyingP.setValue(service)
    f.poll match {
      case Some(Throw(exc: Failure)) =>
        assert(exc.getMessage == "Returned unavailable service")
      case _ => fail()
    }
    verify(service, times(1)).close(any[Time])

    assert(pool.isAvailable)
    when(service.status).thenReturn(Status.Open)
    assert(pool().poll == Some(Return(service)))
    verify(service, times(1)).close(any[Time])
  }

  test("close(): before connection creation") {
    val ctx = new Ctx
    import ctx._

    closeP.setDone()

    assert(pool.close().poll == Some(Return.Unit))
    assertClosed()

    verify(underlying, never).apply(any[ClientConnection])
  }

  test("close(): after connection creation, before underlying") {
    val ctx = new Ctx
    import ctx._

    closeP.setDone()

    val f = pool()
    assert(!f.isDefined)
    verify(underlying, times(1)).apply(any[ClientConnection])

    var exc: Option[Throwable] = None
    underlyingP.setInterruptHandler {
      case exc1 => exc = Some(exc1)
    }

    assert(pool.close().poll == Some(Return.Unit))
    assert(!f.isDefined)
    exc match {
      case Some(_: ServiceClosedException) =>
      case _ => fail()
    }

    verify(service, never).close(any[Time])
    underlyingP.setValue(service)
    verify(service, times(1)).close(any[Time])

    assertClosed()
  }

  test("close(): after connection creation, after underlying") {
    val ctx = new Ctx
    import ctx._

    closeP.setDone()

    val f = pool()
    assert(!f.isDefined)
    underlyingP.setValue(service)
    assert(f.poll == Some(Return(service)))
    verify(service, never).close(any[Time])

    assert(pool.close().poll == Some(Return.Unit))
    assertClosed()
    verify(service, never).close(any[Time])
    assert(Await.result(f).close().poll == Some(Return.Unit))
    verify(service, times(1)).close(any[Time])
  }

  test("close: closes underlying") {
    val ctx = new Ctx
    import ctx._

    val f = pool()
    underlyingP.setValue(service)
    assert(f.isDefined)
    Await.result(f).close()

    verify(underlying, never).close(any[Time])
    
    val close = pool.close()
    assert(!close.isDefined)
    
    // The pool is closed for business, even if it isn't done
    // *closing* yet.
    assertClosed()

    verify(underlying, times(1)).close(any[Time])
    
    closeP.setDone()
    assert(close.isDefined)
  }

  test("does not close, reuses idle connections") {
    val ctx = new Ctx
    import ctx._

    val f = pool()
    assert(!f.isDefined)
    underlyingP.setValue(service)
    assert(f.poll == Some(Return(service)))
    Await.result(Await.result(f).close())

    val g = pool()
    assert(g.poll == Some(Return(service)))
    Await.result(Await.result(g).close())

    verify(underlying, times(1)).apply(any[ClientConnection])
    verify(service, never).close(any[Time])
  }

  test("composes pool and service status") {
    val ctx = new Ctx
    import ctx._
    
    // Not yet initialized.
    assert(pool.status == Status.Open)
    verify(underlying, times(1)).status
    when(underlying.status).thenReturn(Status.Closed)
    assert(pool.status == Status.Closed)

    when(underlying.status).thenReturn(Status.Open)
    underlyingP.setValue(service)

    val s = Await.result(pool())
    assert(pool.status == Status.Open)
    when(service.status).thenReturn(Status.Busy)
    assert(pool.status == Status.Busy)
    when(underlying.status).thenReturn(Status.Closed)
    assert(pool.status == Status.Closed)
    when(underlying.status).thenReturn(Status.Busy)
    
    when(service.status).thenReturn(Status.Open)
    assert(pool.status == Status.Busy)
    when(pool.status).thenReturn(Status.Open)
    assert(pool.status == Status.Open)
    
    // After close, we're forever Closed.
    pool.close()
    assert(pool.status == Status.Closed)
  }

  test("ignore cached but Closed service status") {
    val ctx = new Ctx
    import ctx._
    
    underlyingP.setValue(service)
    val s = Await.result(pool())
    
    // We're checked out; reflect the service status
    when(service.status).thenReturn(Status.Busy)
    assert(pool.status == Status.Busy)
    
    when(service.status).thenReturn(Status.Closed)
    assert(pool.status == Status.Open)
    when(underlying.status).thenReturn(Status.Closed)
    assert(pool.status == Status.Closed)
  }
}
