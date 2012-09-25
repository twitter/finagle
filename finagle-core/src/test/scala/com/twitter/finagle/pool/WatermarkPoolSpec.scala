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
    val service = mock[Service[Int, Int]]
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
      f.release()
      there was one(service).isAvailable
      there was one(service).release()
    }
  }

  "WatermarkPool (lowWatermark = 1, highWatermark = 1)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val service0 = mock[Service[Int, Int]]
    val promise = new Promise[Service[Int, Int]]

    factory() returns Future.value(service0)
    service0.isAvailable returns true

    val pool = new WatermarkPool(factory, 1, 1)

    "enqueue requests when we have already allocated one item" in {
      val f0 = pool()
      f0.isDefined must beTrue
      there was one(factory)()

      val f1 = pool()
      f1.isDefined must beFalse

      f0().release()

      f1.isDefined must beTrue
      there was one(service0).isAvailable
      there was no(service0).release()
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

    "throw CancelledConnectionException if an enqueued waiter is cancelled" in {
      pool().isDefined must beTrue  // consume item
      there was one(factory)()

      val f1 = pool()
      f1.isDefined must beFalse

      f1.cancel()
      f1.isDefined must beTrue
      f1() must throwA(new CancelledConnectionException)
    }

    "when item becomes unhealthy while pool is idle, it is returned" in {
      val f0 = pool()
      f0.isDefined must beTrue
      f0().release()  // give it back
      there was no(service0).release()  // it retained

      val service1 = mock[Service[Int, Int]]
      factory() returns Future.value(service1)
      service0.isAvailable returns false

      val f1 = pool()
      there was one(service0).release()
      f1.isDefined must beTrue
    }

    "when giving an unhealthy item back" in {
      val service1 = mock[Service[Int, Int]]
      val service1Promise = new Promise[Service[Int, Int]]
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
        f0().release()
        there was one(service0).release()
        there was one(service0).isAvailable
        f1.isDefined must beTrue
        there were two(factory)()
        f1()(123)() must be_==(111)

        // Healthy again:
        f1().release()
        there was one(service1).isAvailable
        // No additional disposes.
        there was no(service1).release()
      }

      "propagate cancellation" in {
        f0().release()
        // now we're waiting.
        service1Promise.isCancelled must beFalse
        f1.cancel()
        service1Promise.isCancelled must beTrue
      }
    }
  }

  "WatermarkPool (lowWatermark = 1, highWatermark = 1, maxWaiters = 2)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val service0 = mock[Service[Int, Int]]
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
      f0().release()

      f1.isDefined must beTrue
      there was one(service0).isAvailable
      there was no(service0).release()
    }
  }

  "WatermarkPool (lowWatermark = 100, highWatermark = 1000)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val pool = new WatermarkPool(factory, 100, 1000)

    "maintain at all times up to 100 items" in {
      val mocks = 0 until 100 map { _ => mock[Service[Int, Int]] }

      val services = 0 until 100 map { i =>
        factory() returns Future.value(mocks(i))
        pool()()
      }

      there were 100.times(factory)()
      // We now have 100 items, the low watermark of the pool.  We can
      // give them all back, and all should persist.
      mocks foreach { service =>
        there was no(service).release()
        service.isAvailable returns true
      }

      mocks zip services foreach { case (mock, service) =>
        service.release()
        there was one(mock).isAvailable
        there was no(mock).release()
      }

      // We can now fetch them again, incurring no additional object
      // creation.
      0 until 100 foreach { _ => pool()() }
      mocks foreach { service =>
        there were two(service).isAvailable
      }

      there were 100.times(factory)()
      mocks foreach { service =>
        there was no(service).release()
      }
    }
  }

  "service lifecycle" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val service = mock[Service[Int, Int]]
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


      f().release()
      there was no(service).release()

      factory() returns new Promise[Service[Int, Int]]
      service.isAvailable returns false

      // The service is now unhealty, so it should be discarded, and a
      // new one should be made.
      pool().isDefined must beFalse
      there was one(service).release()
      there were two(factory)()
    }
  }

  "a closed pool" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val pool = new WatermarkPool(factory, 100, 1000)
    val underlyingService = mock[Service[Int, Int]]

    factory() returns Future.value(underlyingService)
    underlyingService.isAvailable returns true
    underlyingService(123) returns Future.value(321)

    val serviceFuture = pool()
    serviceFuture.isDefined must beTrue
    val service = serviceFuture()

    "drain the queue" in {
      service.release()
      there was no(underlyingService).release()
      pool.close()
      there was one(underlyingService).release()
    }

    "release services as they become available" in {
      pool.close()
      there was no(underlyingService).release()
      service.release()
      there was one(underlyingService).release()
    }

    "deny new requests" in {
      pool.close()
      pool()() must throwA[ServiceClosedException]
    }

    "close the underlying factory" in {
      pool.close()
      there was one(factory).close()
    }
  }
}
