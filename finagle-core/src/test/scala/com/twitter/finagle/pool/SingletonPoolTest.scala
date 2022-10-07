package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Time
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

object SingletonPoolTest {
  private def await[T](t: Awaitable[T]): T = Await.result(t, 1.second)

  private class Ctx(allowInterrupts: Boolean = true) extends MockitoSugar {
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
    underlyingP.setInterruptHandler { case t => underlyingP.updateIfEmpty(Throw(t)) }
    when(underlying(any[ClientConnection])).thenReturn(underlyingP)
    val pool = new SingletonPool(underlying, allowInterrupts, NullStatsReceiver)

    def assertClosed(): Unit = {
      val Some(Throw(Failure(Some(cause)))) = pool().poll
      assert(cause.isInstanceOf[ServiceClosedException])
    }
  }
}

class SingletonPoolTest extends AnyFunSuite {
  import SingletonPoolTest._

  test("doesn't leak connections on interruption with (allowInterrupts=false)") {
    val ctx = new Ctx(allowInterrupts = false)
    import ctx._

    when(underlying.status).thenReturn(Status.Open)

    val ex = new Exception("Impatient.")

    // We do it twice, just to make sure there are no surprises.
    val service1 = pool()
    assert(!service1.isDefined)
    service1.raise(ex)
    assert(service1.poll == Some(Throw(ex)))

    val service2 = pool()
    assert(!service2.isDefined)
    service2.raise(ex)
    assert(service2.poll == Some(Throw(ex)))

    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.status).thenReturn(Status.Open)
    when(underlyingService.close()).thenReturn(Future.Done)
    underlyingP.setValue(underlyingService)

    assert(pool.isAvailable)
    pool.close()
    assert(!pool.isAvailable)
    verify(underlyingService, times(1)).close()
    verify(underlying, times(1)).close()
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
    await(fst).close()
    verify(service, times(1)).close(any[Time])

    verify(underlying, times(2)).apply(any[ClientConnection])
    await(snd).close()
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
    assert(await(f).close().poll == Some(Return.Unit))
    verify(service, times(1)).close(any[Time])
  }

  test("close: closes underlying") {
    val ctx = new Ctx
    import ctx._

    val f = pool()
    underlyingP.setValue(service)
    assert(f.isDefined)
    await(f).close()

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
    await(await(f).close())

    val g = pool()
    assert(g.poll == Some(Return(service)))
    await(await(g).close())

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

    val s = await(pool())
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
    val s = await(pool())

    // We're checked out; reflect the service status
    when(service.status).thenReturn(Status.Busy)
    assert(pool.status == Status.Busy)

    when(service.status).thenReturn(Status.Closed)
    assert(pool.status == Status.Open)
    when(underlying.status).thenReturn(Status.Closed)
    assert(pool.status == Status.Closed)
  }

  test("interrupts propagate when allowInterrupts is set") {
    val ctx = new Ctx(allowInterrupts = true)
    import ctx._
    val sf = pool()
    val exc = new Exception("timeout!")
    sf.raise(exc)
    assert(exc == intercept[Exception] { await(sf) })
    assert(exc == intercept[Exception] { await(underlyingP) })
  }

  test("interrupts don't propagate when allowInterrupts is not set") {
    val ctx = new Ctx(allowInterrupts = false)
    import ctx._
    val sf = pool()
    val exc = new Exception("timeout!")
    sf.raise(exc)
    assert(exc == intercept[Exception] { await(sf) })
    // the above interrupt didn't interfere with the underlying
    // service acquisition request!
    underlyingP.setValue(service)
    assert(service == await(underlyingP))
  }
}
