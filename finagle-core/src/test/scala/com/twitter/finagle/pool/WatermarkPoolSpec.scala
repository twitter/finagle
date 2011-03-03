package com.twitter.finagle.pool

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers
import com.twitter.conversions.time._
import com.twitter.util.{Future, Promise, Return, Throw}

import com.twitter.finagle._

object WatermarkPoolSpec extends Specification with Mockito {
  "WatermarkPool (lowWatermark = 0)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val service = mock[Service[Int, Int]]
    val promise = new Promise[Service[Int, Int]]

    factory.make() returns promise
    service.isAvailable returns true
    service(123) returns Future.value(321)
    val pool = new WatermarkPool(factory, 0)

    "yield the pooled item when the underlying factory returns it" in {
      val f = pool.make()
      f.isDefined must beFalse
      there was one(factory).make()
      promise() = Return(service)
      f.isDefined must beTrue
      f()(123)() must be_==(321)
    }

    "dispose of it when returned to the pool" in {
      promise() = Return(service)
      val f = pool.make()()
      f.release()
      there was one(service).isAvailable
      there was one(service).release()
    }
  }

  "WatermarkPool (lowWatermark = 1, highWatermark = 1)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val service0 = mock[Service[Int, Int]]
    val promise = new Promise[Service[Int, Int]]

    factory.make() returns Future.value(service0)
    service0.isAvailable returns true

    val pool = new WatermarkPool(factory, 1, 1)

    "enqueue requests when we have already allocated one item" in {
      val f0 = pool.make()
      f0.isDefined must beTrue
      there was one(factory).make()

      val f1 = pool.make()
      f1.isDefined must beFalse

      f0().release()

      f1.isDefined must beTrue
      there was one(service0).isAvailable
      there was no(service0).release()
    }

    "make a new item if the returned item is unhealthy" in {
      val service1 = mock[Service[Int, Int]]
      service1(123) returns Future.value(111)

      val f0 = pool.make()
      f0.isDefined must beTrue
      there was one(factory).make()

      val f1 = pool.make()
      f1.isDefined must beFalse

      factory.make() returns Future.value(service1)
      service0.isAvailable returns false
      service1.isAvailable returns true

      f0().release()
      there was one(service0).release()
      there was one(service0).isAvailable
      f1.isDefined must beTrue
      there were two(factory).make()
      f1()(123)() must be_==(111)

      // Healthy again:
      f1().release()
      there was one(service1).isAvailable
      // No additional disposes.
      there was no(service1).release()
    }
  }

  "WatermarkPool (lowWatermark = 100, highWatermark = 1000)" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val pool = new WatermarkPool(factory, 100, 1000)

    "maintain at all times up to 100 items" in {
      val mocks = 0 until 100 map { _ => mock[Service[Int, Int]] }

      val services = 0 until 100 map { i =>
        factory.make() returns Future.value(mocks(i))
        pool.make()()
      }

      there were 100.times(factory).make()
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
      0 until 100 foreach { _ => pool.make()() }
      mocks foreach { service =>
        there were two(service).isAvailable
      }

      there were 100.times(factory).make()
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
        factory.make() returns promise
        promise() = Throw(new Exception)
        pool.make().isThrow mustBe true
      }

      val promise = new Promise[Service[Int, Int]]
      factory.make() returns promise
      promise() = Return(service)
      pool.make()(1.second).isAvailable mustBe true
    }

    "release unhealthy services that have been queued" in {
      val promise = new Promise[Service[Int, Int]]
      factory.make() returns promise

      promise() = Return(service)

      val f = pool.make()
      there was one(factory).make()
      f.isDefined must beTrue
      f().isAvailable must beTrue


      f().release()
      there was no(service).release()

      factory.make() returns new Promise[Service[Int, Int]]
      service.isAvailable returns false

      // The service is now unhealty, so it should be discarded, and a
      // new one should be made.
      pool.make().isDefined must beFalse
      there was one(service).release()
      there were two(factory).make()
    }
  }

  "a closed pool" should {
    val factory = mock[ServiceFactory[Int, Int]]
    val pool = new WatermarkPool(factory, 100, 1000)
    val underlyingService = mock[Service[Int, Int]]

    factory.make() returns Future.value(underlyingService)
    underlyingService.isAvailable returns true
    underlyingService(123) returns Future.value(321)

    val serviceFuture = pool.make()
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
      pool.make()() must throwA[ServiceClosedException]
    }

    "close the underlying factory" in {
      pool.close()
      there was one(factory).close()
    }
  }
}
