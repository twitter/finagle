package com.twitter.finagle.pool

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.mockito.Matchers
import com.twitter.conversions.time._
import com.twitter.util.{Future, Promise, Return, Throw}

import com.twitter.finagle._

class WatermarkPoolSpec extends SpecificationWithJUnit with Mockito {
  "WatermarkPool" should {
    val factory = mock[ServiceFactory[Int, Int]]
    factory.close(any) returns Future.Done
    val pool = new WatermarkPool(factory, 0)

    "reflect the underlying availability" in {
      factory.isAvailable returns true
      pool.isAvailable must beTrue
      there was one(factory).isAvailable

      factory.isAvailable returns false
      pool.isAvailable must beFalse
      there were two(factory).isAvailable
    }
  }

  "WatermarkPool (lowWatermark = 0)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    factory.close(any) returns Future.Done
    val service = mock[Service[Int, Int]]
    service.close(any) returns Future.Done
    val promise = new Promise[Service[Int, Int]]

    factory() returns promise
    service.isAvailable returns true
    service(123) returns Future.value(321)
    val pool = new WatermarkPool(factory, 0)

    "yield the pooled item when the underlying factory returns it" in {
      val f = pool()
      f.isDefined must beFalse
      there was one(factory)()
      promise() = Return(service)
      f.isDefined must beTrue
      f()(123)() must be_==(321)
    }

    "dispose of it when returned to the pool" in {
      promise() = Return(service)
      val f = pool()()
      f.close()
      there was one(service).isAvailable
      there was one(service).close(any)
    }
  }

  "WatermarkPool (lowWatermark = 1, highWatermark = 1)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val service0 = mock[Service[Int, Int]]
    val promise = new Promise[Service[Int, Int]]
    
    factory.close(any) returns Future.Done
    service0.close(any) returns Future.Done

    factory() returns Future.value(service0)
    service0.isAvailable returns true

    val pool = new WatermarkPool(factory, 1, 1)

    "enqueue requests when we have already allocated one item" in {
      val f0 = pool()
      f0.isDefined must beTrue
      there was one(factory)()

      val f1 = pool()
      f1.isDefined must beFalse

      f0().close()

      f1.isDefined must beTrue
      there was one(service0).isAvailable
      there was no(service0).close(any)
    }

    "retry an enqueued request if the underlying factory fails" in {
      val p = new Promise[Service[Int, Int]]
      factory() returns p

      val f0 = pool()
      f0.isDefined must beFalse
      there was one(factory)()

      // The second one will be enqueued
      val f1 = pool()
      f1.isDefined must beFalse
      there was one(factory)()

      // Fail the request, which should dequeue
      // the queued one.
      factory() returns Future.value(service0)
      val exc = new Exception
      p() = Throw(exc)
      f0.poll must beSome(Throw(exc))

      there were two(factory)()
      f1.poll must beSome(Return(service0))
    }

    "throw if an enqueued waiter is cancelled" in {
      pool().isDefined must beTrue  // consume item
      there was one(factory)()

      val f1 = pool()
      f1.isDefined must beFalse

      val cause = new Exception
      f1.raise(cause)
      f1.isDefined must beTrue
      f1() must throwA(new CancelledConnectionException)
    }

    "when item becomes unhealthy while pool is idle, it is returned" in {
      val f0 = pool()
      f0.isDefined must beTrue
      f0().close()  // give it back
      there was no(service0).close(any)  // it retained

      val service1 = mock[Service[Int, Int]]
      service1.close(any) returns Future.Done
      factory() returns Future.value(service1)
      service0.isAvailable returns false

      val f1 = pool()
      there was one(service0).close(any)
      f1.isDefined must beTrue
    }

    "when giving an unhealthy item back" in {
      val service1 = mock[Service[Int, Int]]
      service1.close(any) returns Future.Done
      val service1Promise = new Promise[Service[Int, Int]] {
        @volatile var interrupted: Option[Throwable] = None
        setInterruptHandler { case exc => interrupted = Some(exc) }
      }
      service1(123) returns Future.value(111)

      val f0 = pool()
      f0.isDefined must beTrue
      there was one(factory)()

      val f1 = pool()
      f1.isDefined must beFalse

      factory() returns service1Promise
      service0.isAvailable returns false
      service1.isAvailable returns true

      "make a new item" in {
        service1Promise() = Return(service1)
        f0().close()
        there was one(service0).close(any)
        there was one(service0).isAvailable
        f1.isDefined must beTrue
        there were two(factory)()
        f1()(123)() must be_==(111)

        // Healthy again:
        f1().close()
        there was one(service1).isAvailable
        // No additional disposes.
        there was no(service1).close(any)
      }

      "propagate interrupts" in {
        f0().close()
        // now we're waiting.
        service1Promise.interrupted must beNone
        val exc = new Exception
        f1.raise(exc)
        service1Promise.interrupted must beSome(exc)
      }
    }
  }

  "WatermarkPool (lowWatermark = 1, highWatermark = 1, maxWaiters = 2)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    factory.close(any) returns Future.Done
    val service0 = mock[Service[Int, Int]]
    service0.close(any) returns Future.Done
    val promise = new Promise[Service[Int, Int]]

    factory() returns Future.value(service0)
    service0.isAvailable returns true

    val pool = new WatermarkPool(factory, 1, 1, maxWaiters = 2)

    "throw TooManyWaitersException when the number of waiters exceeds 2" in {
      val f0 = pool()
      f0.isDefined must beTrue
      there was one(factory)()

      // one waiter. this is cool.
      val f1 = pool()
      f1.isDefined must beFalse

      // two waiters. this is *still* cool.
      val f2 = pool()
      f2.isDefined must beFalse

      // three waiters and i freak out.
      val f3 = pool()
      f3.isDefined must beTrue
      f3() must throwA[TooManyWaitersException]

      // give back my original item, and f1 should still get something.
      f0().close()

      f1.isDefined must beTrue
      there was one(service0).isAvailable
      there was no(service0).close(any)
    }
  }

  "WatermarkPool (lowWatermark = 100, highWatermark = 1000)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    factory.close(any) returns Future.Done
    val pool = new WatermarkPool(factory, 100, 1000)

    "maintain at all times up to 100 items" in {
      val mocks = 0 until 100 map { _ =>
        val s = mock[Service[Int, Int]]
        s.close(any) returns Future.Done
        s
      }

      val services = 0 until 100 map { i =>
        factory() returns Future.value(mocks(i))
        pool()()
      }

      there were 100.times(factory)()
      // We now have 100 items, the low watermark of the pool.  We can
      // give them all back, and all should persist.
      mocks foreach { service =>
        there was no(service).close(any)
        service.isAvailable returns true
      }

      mocks zip services foreach { case (mock, service) =>
        service.close()
        there was one(mock).isAvailable
        there was no(mock).close(any)
      }

      // We can now fetch them again, incurring no additional object
      // creation.
      0 until 100 foreach { _ => pool()() }
      mocks foreach { service =>
        there were two(service).isAvailable
      }

      there were 100.times(factory)()
      mocks foreach { service =>
        there was no(service).close(any)
      }
    }
  }

  "service lifecycle" should {
    val factory = mock[ServiceFactory[Int, Int]]
    factory.close(any) returns Future.Done
    val service = mock[Service[Int, Int]]
    service.close(any) returns Future.Done
    service.isAvailable returns true
    val highWaterMark = 5
    val pool = new WatermarkPool(factory, 1, highWaterMark)

    "not leak services when they are born unhealthy" in {
      (0 until highWaterMark) foreach { _ =>
        val promise = new Promise[Service[Int, Int]]
        factory() returns promise
        promise() = Throw(new Exception)
        pool().isThrow mustBe true
      }

      val promise = new Promise[Service[Int, Int]]
      factory() returns promise
      promise() = Return(service)
      pool()(1.second).isAvailable mustBe true
    }

    "release unhealthy services that have been queued" in {
      val promise = new Promise[Service[Int, Int]]
      factory() returns promise

      promise() = Return(service)

      val f = pool()
      there was one(factory)()
      f.isDefined must beTrue
      f().isAvailable must beTrue


      f().close()
      there was no(service).close(any)

      factory() returns new Promise[Service[Int, Int]]
      service.isAvailable returns false

      // The service is now unhealty, so it should be discarded, and a
      // new one should be made.
      pool().isDefined must beFalse
      there was one(service).close(any)
      there were two(factory)()
    }
  }

  "a closed pool" should {
    val factory = mock[ServiceFactory[Int, Int]]
    factory.close(any) returns Future.Done
    val pool = new WatermarkPool(factory, 100, 1000)
    val underlyingService = mock[Service[Int, Int]]
    underlyingService.close(any) returns Future.Done

    factory() returns Future.value(underlyingService)
    underlyingService.isAvailable returns true
    underlyingService(123) returns Future.value(321)

    val serviceFuture = pool()
    serviceFuture.isDefined must beTrue
    val service = serviceFuture()

    "drain the queue" in {
      service.close()
      there was no(underlyingService).close(any)
      pool.close()
      there was one(underlyingService).close(any)
    }

    "release services as they become available" in {
      pool.close()
      there was no(underlyingService).close(any)
      service.close()
      there was one(underlyingService).close(any)
    }

    "deny new requests" in {
      pool.close()
      pool()() must throwA[ServiceClosedException]
    }

    "close the underlying factory" in {
      pool.close()
      there was one(factory).close(any)
    }
  }
}
