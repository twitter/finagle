package com.twitter.finagle.pool

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Await
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
import scala.language.reflectiveCalls
import org.scalatest.funspec.AnyFunSpec

class WatermarkPoolTest extends AnyFunSpec with MockitoSugar {
  describe("A WatermarkPool") {
    it("should reflect the underlying availability") {
      val factory = mock[ServiceFactory[Int, Int]]
      when(factory.close(any[Time])).thenReturn(Future.Done)
      val pool = new WatermarkPool(factory, 0)

      when(factory.status).thenReturn(Status.Open)
      assert(pool.isAvailable)
      verify(factory).status

      when(factory.status).thenReturn(Status.Closed)
      assert(!pool.isAvailable)
      verify(factory, times(2)).status
    }
  }

  describe("WatermarkPool (lowWatermark = 0)") {
    trait WatermarkPoolLowZero {
      val factory = mock[ServiceFactory[Int, Int]]
      when(factory.close(any[Time])).thenReturn(Future.Done)
      val service = mock[Service[Int, Int]]
      when(service.close(any[Time])).thenReturn(Future.Done)
      val promise = new Promise[Service[Int, Int]]

      when(factory()).thenReturn(promise)
      when(service.status).thenReturn(Status.Open)
      when(service(123)).thenReturn(Future.value(321))
      val pool = new WatermarkPool(factory, 0)
    }

    it("should yield the pooled item when the underlying factory returns it") {
      new WatermarkPoolLowZero {
        val f = pool()
        assert(!f.isDefined)
        verify(factory)()
        promise() = Return(service)
        assert(f.isDefined)
        assert(Await.result(Await.result(f)(123)) == 321)
      }
    }

    it("should dispose of it when returned to the pool") {
      new WatermarkPoolLowZero {
        promise() = Return(service)
        val f = Await.result(pool())
        f.close()
        verify(service).status
        verify(service).close(any[Time])
      }
    }
  }

  describe("WatermarkPool (lowWatermark = 1, highWatermark = 1)") {
    trait WatermarkPoolLowOneHighOne {
      val factory = mock[ServiceFactory[Int, Int]]
      val service0 = mock[Service[Int, Int]]
      val promise = new Promise[Service[Int, Int]]

      when(factory.close(any[Time])).thenReturn(Future.Done)
      when(service0.close(any[Time])).thenReturn(Future.Done)

      when(factory()).thenReturn(Future.value(service0))
      when(service0.status).thenReturn(Status.Open)

      val pool = new WatermarkPool(factory, 1, 1)
    }

    it("should enqueue requests when we have already allocated one item") {
      new WatermarkPoolLowOneHighOne {
        val f0 = pool()
        assert(f0.isDefined)
        verify(factory)()

        val f1 = pool()
        assert(!f1.isDefined)

        Await.result(f0).close()

        assert(f1.isDefined)
        verify(service0).status
        verify(service0, never()).close(any[Time])
      }
    }

    it("should retry an enqueued request if the underlying factory fails") {
      new WatermarkPoolLowOneHighOne {
        val p = new Promise[Service[Int, Int]]
        when(factory()).thenReturn(p)

        val f0 = pool()
        assert(!f0.isDefined)
        verify(factory)()

        // The second one will be enqueued
        val f1 = pool()
        assert(!f1.isDefined)
        verify(factory)()

        // Fail the request, which should dequeue
        // the queued one.
        when(factory()).thenReturn(Future.value(service0))
        val exc = new Exception
        p() = Throw(exc)
        assert(f0.poll == Some(Throw(exc)))

        verify(factory, times(2))()
        assert(f1.poll == Some(Return(service0)))
      }
    }

    it("should throw if an enqueued waiter is cancelled") {
      new WatermarkPoolLowOneHighOne {
        assert(pool().isDefined) // consume item
        verify(factory)()

        val f1 = pool()
        assert(!f1.isDefined)

        val cause = new Exception
        f1.raise(cause)
        assert(f1.isDefined)
        val failure = intercept[Failure] { Await.result(f1) }
        assert(failure.getCause.isInstanceOf[CancelledConnectionException])
        assert(failure.isFlagged(FailureFlags.Interrupted))
      }
    }

    it("when item becomes unhealthy while pool is idle, it is returned") {
      new WatermarkPoolLowOneHighOne {
        val f0 = pool()
        assert(f0.isDefined)
        Await.result(f0).close() // give it back
        verify(service0, never()).close(any[Time]) // it retained

        val service1 = mock[Service[Int, Int]]
        when(service1.close(any[Time])).thenReturn(Future.Done)
        when(factory()).thenReturn(Future.value(service1))
        when(service0.status).thenReturn(Status.Closed)

        val f1 = pool()
        verify(service0).close(any[Time])
        assert(f1.isDefined)
      }
    }

    it("when giving an unhealthy item back") {
      new WatermarkPoolLowOneHighOne {
        val service1 = mock[Service[Int, Int]]
        when(service1.close(any[Time])).thenReturn(Future.Done)
        val service1Promise = new Promise[Service[Int, Int]]
        when(service1(123)).thenReturn(Future.value(111))

        val f0 = pool()
        assert(f0.isDefined)
        verify(factory)()

        val f1 = pool()
        assert(!f1.isDefined)

        when(factory()).thenReturn(service1Promise)
        when(service0.status).thenReturn(Status.Closed)
        when(service1.status).thenReturn(Status.Open)

        // "make a new item"
        service1Promise() = Return(service1)
        Await.result(f0).close()
        verify(service0).close(any[Time])
        verify(service0).status
        assert(f1.isDefined)
        verify(factory, times(2))()
        assert(Await.result(Await.result(f1)(123)) == 111)

        // Healthy again:
        Await.result(f1).close()
        verify(service1).status
        // No additional disposes.
        verify(service1, never()).close(any[Time])
      }
    }
  }

  describe("WatermarkPool (lowWatermark = 1, highWatermark = 1, maxWaiters = 2)") {
    val factory = mock[ServiceFactory[Int, Int]]
    when(factory.close(any[Time])).thenReturn(Future.Done)
    val service0 = mock[Service[Int, Int]]
    when(service0.close(any[Time])).thenReturn(Future.Done)

    when(factory()).thenReturn(Future.value(service0))
    when(service0.status).thenReturn(Status.Open)

    val statsRecv = new InMemoryStatsReceiver
    val pool = new WatermarkPool(factory, 1, 1, statsRecv, maxWaiters = 2)
    def numWaited() = statsRecv.counter("pool_num_waited")()
    def numTooManyWaiters() = statsRecv.counter("pool_num_too_many_waiters")()

    it("should throw TooManyWaitersException when the number of waiters exceeds 2") {
      assert(0 == numWaited())
      assert(0 == numTooManyWaiters())
      val f0 = pool()
      assert(f0.isDefined)
      verify(factory)()

      // one waiter. this is cool.
      val f1 = pool()
      assert(!f1.isDefined)
      assert(1 == numWaited())
      assert(0 == numTooManyWaiters())

      // two waiters. this is *still* cool.
      val f2 = pool()
      assert(!f2.isDefined)
      assert(2 == numWaited())
      assert(0 == numTooManyWaiters())

      // three waiters and i freak out.
      val f3 = pool()
      assert(f3.isDefined)
      intercept[TooManyWaitersException] { Await.result(f3) }
      assert(2 == numWaited())
      assert(1 == numTooManyWaiters())

      // give back my original item, and f1 should still get something.
      Await.result(f0).close()

      assert(f1.isDefined)
      verify(service0).status
      verify(service0, never()).close(any[Time])
    }
  }

  describe("WatermarkPool (lowWatermark = 100, highWatermark = 1000)") {
    val factory = mock[ServiceFactory[Int, Int]]
    when(factory.close(any[Time])).thenReturn(Future.Done)
    val pool = new WatermarkPool(factory, 100, 1000)

    val mocks = 0 until 100 map { _ =>
      val s = mock[Service[Int, Int]]
      when(s.close(any[Time])).thenReturn(Future.Done)
      s
    }

    it("should persist 100 connections") {
      val services = 0 until 100 map { i =>
        when(factory()).thenReturn(Future.value(mocks(i)))
        Await.result(pool())
      }

      verify(factory, times(100))()
      // We now have 100 items, the low watermark of the pool.  We can
      // give them all back, and all should persist.
      mocks foreach { service =>
        verify(service, never()).close(any[Time])
        when(service.status).thenReturn(Status.Open)
      }

      mocks zip services foreach {
        case (mock, service) =>
          service.close()
          verify(mock).status
          verify(mock, never()).close(any[Time])
      }
    }

    it("should return the cached connections for the next 100 apply calls") {
      // We can now fetch them again, incurring no additional object
      // creation.
      0 until 100 foreach { _ => Await.result(pool()) }
      mocks foreach { service => verify(service, times(2)).status }

      verify(factory, times(100))()
      mocks foreach { service => verify(service, never()).close(any[Time]) }
    }
  }

  trait WatermarkPoolLowOneHighFive {
    val sr = new InMemoryStatsReceiver
    val factory = mock[ServiceFactory[Int, Int]]
    when(factory.close(any[Time])).thenReturn(Future.Done)
    val service = mock[Service[Int, Int]]
    when(service.close(any[Time])).thenReturn(Future.Done)
    when(service.status).thenReturn(Status.Open)
    when(service(123)).thenReturn(Future.value(321))
    val highWaterMark = 5
    val pool = new WatermarkPool(factory, 1, highWaterMark, sr)
  }

  describe("Watermark service lifecycle") {
    it("is resilient to multiple closes on a single service instance") {
      val svcFac = new ServiceFactory[Int, Int] {
        def apply(conn: ClientConnection): Future[Service[Int, Int]] =
          Future.value(Service.mk[Int, Int](Future.value(_)))

        def close(deadline: Time): Future[Unit] = Future.Done
        def status: Status = Status.Open
      }
      val sr = new InMemoryStatsReceiver
      val wmp = new WatermarkPool[Int, Int](svcFac, 0, 5, sr)

      val svc1 = Await.result(wmp(), 5.seconds)
      assert(wmp.size == 1)
      val svc2 = Await.result(wmp(), 5.seconds)
      assert(wmp.size == 2)
      val svc3 = Await.result(wmp(), 5.seconds)
      assert(wmp.size == 3)

      Await.ready(svc3.close(), 5.seconds)
      assert(wmp.size == 2)

      // closing the same instance again, is a no-op
      Await.ready(svc3.close(), 5.seconds)
      assert(wmp.size == 2)

      val svc4 = Await.result(wmp(), 5.seconds)
      assert(wmp.size == 3)

      // another no-op on the already closed svc3
      Await.ready(svc3.close(), 5.seconds)
      assert(wmp.size == 3)

      // first close on svc4
      Await.ready(svc4.close(), 5.seconds)
      assert(wmp.size == 2)

      // first close on svc2
      Await.ready(svc2.close(), 5.seconds)
      assert(wmp.size == 1)

      // first close on svc1
      Await.ready(svc1.close(), 5.seconds)
      assert(wmp.size == 0)
    }

    it("allows service reuse without messing up pool accounting") {
      val svcFac = new ServiceFactory[Int, Int] {
        def apply(conn: ClientConnection): Future[Service[Int, Int]] =
          Future.value(Service.mk[Int, Int](Future.value(_)))

        def close(deadline: Time): Future[Unit] = Future.Done
        def status: Status = Status.Open
      }
      val sr = new InMemoryStatsReceiver
      val wmp = new WatermarkPool[Int, Int](svcFac, 0, 1, sr)

      assert(wmp.size == 0)
      val svc1 = Await.result(wmp(), 5.seconds)
      assert(wmp.size == 1)
      val waitingSvc = wmp()
      Await.result(svc1.close(), 5.seconds)
      assert(wmp.size == 1) // we have a waiter so the service should go back in the pool
      val svc2 = Await.result(waitingSvc, 5.seconds)

      // double-closing first service is a no-op
      Await.ready(svc1.close(), 5.seconds)
      assert(wmp.size == 1)

      Await.result(svc2.close(), 5.seconds)
      assert(wmp.size == 0)
    }

    it("should not leak services when they are born unhealthy") {
      new WatermarkPoolLowOneHighFive {
        (0 until highWaterMark) foreach { _ =>
          val promise = new Promise[Service[Int, Int]]
          when(factory()).thenReturn(promise)
          promise() = Throw(new Exception)
          assert(Await.ready(pool()).poll.get.isThrow)
        }

        val promise = new Promise[Service[Int, Int]]
        when(factory()).thenReturn(promise)
        promise() = Return(service)
        assert(Await.result(pool(), 1.second).status == Status.Open)
      }
    }

    it("should release unhealthy services that have been queued") {
      new WatermarkPoolLowOneHighFive {
        val promise = new Promise[Service[Int, Int]]
        when(factory()).thenReturn(promise)

        promise() = Return(service)

        val f = pool()
        verify(factory)()
        assert(f.isDefined)
        assert(Await.result(f).status == Status.Open)

        Await.result(f).close()
        verify(service, never()).close(any[Time])

        when(factory()).thenReturn(new Promise[Service[Int, Int]])
        when(service.status).thenReturn(Status.Closed)

        // The service is now unhealthy, so it should be discarded, and a
        // new one should be made.
        assert(!pool().isDefined)
        verify(service).close(any[Time])
        verify(factory, times(2))()
      }
    }
  }

  describe("WatermarkPool does not propagate interrupts") {
    it("should cache the connection when it comes back") {
      new WatermarkPoolLowOneHighFive {
        val slowService = new Promise[Service[Int, Int]] {
          @volatile var interrupted: Option[Throwable] = None
          setInterruptHandler { case exc => interrupted = Some(exc) }
        }
        when(factory()).thenReturn(slowService)

        val f = pool()
        verify(factory)()
        val exc = new Exception("giving up")
        f.raise(exc)
        assert(f.isDefined)
        f onFailure {
          case WriteException(e) => assert(e == exc)
          case _ => assert(false, "expecting a WriteException, gets something else")
        }
        assert(slowService.interrupted == None)

        slowService.setValue(service)
        verify(service, never()).close(any[Time])

        val f1 = pool()
        verify(factory)()
        assert(f1.isDefined)
        assert(Await.result(Await.result(f1)(123)) == 321)
      }
    }

    it("service cancellation does not affect # of services") {
      val factory = mock[ServiceFactory[Int, Int]]
      when(factory.close(any[Time])).thenReturn(Future.Done)
      val lowWatermark = 5
      val highWatermark = 10
      val maxWaiters = 3
      val pool = new WatermarkPool(factory, lowWatermark, highWatermark, maxWaiters = maxWaiters)

      val services = 0 until highWatermark map { _ => new Promise[Service[Int, Int]] }
      val wrappedServices = services map { s =>
        when(factory()).thenReturn(s)
        pool()
      }
      0 until maxWaiters map { _ => pool() }
      val f = pool()
      assert(f.isDefined)
      intercept[TooManyWaitersException] { Await.result(f) }

      // # of services does not change after the cancellation
      wrappedServices foreach { _.raise(new Exception) }
      val f1 = pool()
      assert(f1.isDefined)
      intercept[TooManyWaitersException] { Await.result(f1) }
    }
  }

  describe("a closed pool") {
    it("should drain the queue") {
      new WatermarkPoolLowOneHighFive {
        when(factory()).thenReturn(Future.value(service))
        val s = Await.result(pool())
        assert(pool.size == 1)
        s.close()
        verify(service, never()).close(any[Time])
        pool.close()
        assert(pool.size == 0)
        verify(service).close(any[Time])
      }
    }

    it("should release services as they become available") {
      new WatermarkPoolLowOneHighFive {
        when(factory()).thenReturn(Future.value(service))
        val s = Await.result(pool())
        pool.close()
        verify(service, never()).close(any[Time])
        service.close()
        verify(service).close(any[Time])
      }
    }

    it("should deny new requests") {
      new WatermarkPoolLowOneHighFive {
        pool.close()
        intercept[ServiceClosedException] { Await.result(pool()) }
      }
    }

    it("should close the underlying factory") {
      new WatermarkPoolLowOneHighFive {
        pool.close()
        verify(factory).close(any[Time])
      }
    }
  }
}
