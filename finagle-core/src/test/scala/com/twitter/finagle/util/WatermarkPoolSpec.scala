package com.twitter.finagle.util

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import com.twitter.util.{Future, Promise, Return}

object WatermarkPoolSpec extends Specification with Mockito {
  "WatermarkPool (lowWatermark = 0)" should {
    val factory = mock[PoolFactory[Int]]
    val promise = new Promise[Int]

    factory.make() returns promise
    factory.isHealthy(1) returns true
    val pool = new WatermarkPool(factory, 0)

    "yield the pooled item when the underlying factory returns it" in {
      val f = pool.reserve()
      f.isDefined must beFalse
      there was one(factory).make()
      promise() = Return(1)
      f.isDefined must beTrue
      f() must be_==(1)
    }

    "dispose of it when returned to the pool" in {
      promise() = Return(1)
      val f = pool.reserve()
      pool.release(f())
      there was one(factory).isHealthy(1)
      there was one(factory).dispose(1)
    }
  }

  "WatermarkPool (lowWatermark = 1, highWatermark = 1)" should {
    val factory = mock[PoolFactory[Int]]
    factory.make() returns Future.value(1)
    factory.isHealthy(1) returns true

    val pool = new WatermarkPool(factory, 1, 1)

    "enqueue requests when we have already allocated one item" in {
      val f0 = pool.reserve()
      f0.isDefined must beTrue
      there was one(factory).make()

      val f1 = pool.reserve()
      f1.isDefined must beFalse

      pool.release(1)

      f1.isDefined must beTrue
      there was one(factory).isHealthy(1)
      there was one(factory).make()
      there was no(factory).dispose(Matchers.anyInt)
    }

    "make a new item if the returned item is unhealthy" in {
      val f0 = pool.reserve()
      f0.isDefined must beTrue
      there was one(factory).make()

      val f1 = pool.reserve()
      f1.isDefined must beFalse

      factory.make() returns Future.value(2)
      factory.isHealthy(1) returns false
      factory.isHealthy(2) returns true

      pool.release(f0())
      there was one(factory).dispose(f0())
      there was one(factory).isHealthy(f0())
      there were two(factory).make()
      f1.isDefined must beTrue
      f1() must be_==(2)

      // Healthy again:
      pool.release(f1())
      there was one(factory).isHealthy(2)
      // No additional disposes.
      there was one(factory).dispose(Matchers.anyInt)
    }
  }

  "WatermarkPool (lowWatermark = 100, highWatermark = 1000)" should {
    val factory = mock[PoolFactory[Int]]
    val pool = new WatermarkPool(factory, 100, 1000)

    "maintain at all times up to 100 items" in {
      val items = 0 until 100 map { i => 
        factory.make() returns Future.value(i)
        pool.reserve()()
      }

      there were 100.times(factory).make()
      there was no(factory).dispose(Matchers.anyInt)

      // We now have 100 items, the low watermark of the pool.  We can
      // give them all back, and all should persist.
      factory.isHealthy(Matchers.anyInt) returns true

      items foreach { pool.release(_) }
      there were 100.times(factory).isHealthy(Matchers.anyInt)

      there was no(factory).dispose(Matchers.anyInt)

      // We can now fetch them again, incurring no additional object
      // creation.
      0 until 100 foreach { _ => pool.reserve()() }
      there were 200.times(factory).isHealthy(Matchers.anyInt)
      there were 100.times(factory).make()
      there was no(factory).dispose(Matchers.anyInt)
    }
  }
}
