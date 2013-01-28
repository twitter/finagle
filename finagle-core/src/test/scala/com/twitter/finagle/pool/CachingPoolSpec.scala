package com.twitter.finagle.pool

import collection.mutable.Queue

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import com.twitter.util
import com.twitter.util.{Time, Duration, Future, Promise}
import com.twitter.conversions.time._

import com.twitter.finagle.{Service, ServiceFactory, WriteException, MockTimer}

class CachingPoolSpec extends SpecificationWithJUnit with Mockito {
  "CachingPool" should {
    val timer = new MockTimer
    val obj = mock[Object]
    val underlying = mock[ServiceFactory[Any, Any]]
    val underlyingService = mock[Service[Any, Any]]
    underlyingService.isAvailable returns true
    underlyingService(Matchers.any) returns Future.value(obj)
    underlying() returns Future.value(underlyingService)

    "reflect the underlying factory availability" in {
      val pool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      underlying.isAvailable returns false
      pool.isAvailable must beFalse
      there was one(underlying).isAvailable
      underlying.isAvailable returns true
      pool.isAvailable must beTrue
      there were two(underlying).isAvailable
    }

    "cache objects for the specified amount of time" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)

        val f = cachingPool()()
        f(123)() must be_==(obj)
        there was one(underlying)()
        timer.tasks must beEmpty

        f.release()
        there was one(underlyingService).isAvailable
        there was no(underlyingService).release()
        timer.tasks must haveSize(1)
        timer.tasks.head.when must be_==(Time.now + 5.seconds)

        // Reap!
        timeControl.advance(5.seconds)
        timer.tick()
        there was one(underlyingService).release()

        timer.tasks must beEmpty
      }
    }

    "reuse cached objects & revive from death row" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
        cachingPool()().release()
        timer.tasks must haveSize(1)

        there was one(underlying)()
        there was no(underlyingService).release()
        timer.tasks must haveSize(1)

        timeControl.advance(4.seconds)

        cachingPool()().release()
        there was one(underlying)()
        there was no(underlyingService).release()
        timer.tasks must haveSize(1)

        // Originally scheduled time.
        timeControl.advance(1.second)
        timer.tick()

        timer.tasks must haveSize(1)  // reschedule
        there was no(underlyingService).release()

        timer.tasks.head.when must be_==(Time.now + 4.seconds)
        timeControl.advance(5.seconds)
        timer.tick()
        timer.tasks must beEmpty

        there was one(underlyingService).release()
      }
    }

    "handle multiple objects, expiring them only after they are due to" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val o0 = mock[Object]
        val o1 = mock[Object]
        val o2 = mock[Object]

        val s0 = mock[Service[Any, Any]]; s0(any) returns Future.value(o0)
        val s1 = mock[Service[Any, Any]]; s1(any) returns Future.value(o1)
        val s2 = mock[Service[Any, Any]]; s2(any) returns Future.value(o2)

        val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
        underlying() returns Future.value(s0)
        val f0 = cachingPool()()
        f0(123)() must be_==(o0)

        underlying() returns Future.value(s1)
        val f1 = cachingPool()()
        f1(123)() must be_==(o1)

        underlying() returns Future.value(s2)
        val f2 = cachingPool()()
        f2(123)() must be_==(o2)

        val ss = Seq(s0, s1, s2)
        val fs = Seq(f0, f1, f2)

        there were three(underlying)()

        ss foreach { _.isAvailable returns true }

        fs foreach { f =>
          timeControl.advance(5.second)
          f.release()
        }

        timer.tasks must haveSize(1)
        ss foreach { s => there was no(s).release() }

        timer.tick()

        there was one(s0).release()
        there was one(s1).release()
        there was no(s2).release()

        timer.tasks must haveSize(1)
        timer.tick()

        timer.tasks.head.when must be_==(Time.now + 5.seconds)

        // Take it!
        cachingPool()()(123)() must be_==(o2)

        timeControl.advance(5.seconds)

        timer.tick()

        // Nothing left.
        timer.tasks must beEmpty
      }
    }

    "restart timers when a dispose occurs" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val underlyingService = mock[Service[Any, Any]]
        underlyingService.isAvailable returns true
        underlyingService(Matchers.any) returns Future.value(obj)

        val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
        underlying() returns Future.value(underlyingService)

        timer.tasks must beEmpty
        val service = cachingPool()()
        service(123)() must be_==(obj)
        timer.tasks must beEmpty

        service.release()
        timer.tasks must haveSize(1)
        there was no(underlyingService).release()

        timer.tasks.head.when must be_==(Time.now + 5.seconds)

        timeControl.advance(1.second)

        cachingPool()()(123)() must be_==(obj)

        timeControl.advance(4.seconds)
        timer.tasks must beEmpty

        timer.tick()

        there was no(underlyingService).release()

        service.release()

        there was no(underlyingService).release()
        timer.tasks must haveSize(1)
      }
    }

    "don't cache unhealthy objects" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
        val underlyingService = mock[Service[Any, Any]]
        underlyingService(Matchers.any) returns Future.value(obj)
        underlying() returns Future.value(underlyingService)
        underlyingService.isAvailable returns false

        val service = cachingPool()()
        service(123)() must be_==(obj)

        service.release()
        there was one(underlyingService).isAvailable
        there was one(underlyingService).release()

        // No need to clean up an already disposed object.
        timer.tasks must beEmpty
      }
    }

    "cache objects when client sends interrupt" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
        val underlyingService = mock[Service[Any, Any]]
        val slowService = new Promise[Service[Any, Any]]
        underlying() returns slowService

        val service1 = cachingPool()
        val exception = new Exception("give up")
        service1.raise(exception)
        service1() must throwA[WriteException]
        service1 onFailure {
          case WriteException(e) => e mustEqual exception
          case _ => assert(false, "exception was not write exception")
        }

        slowService.setValue(underlyingService)

        val service2 = cachingPool()
        service2.isDefined must beTrue

        // not sure how else to verify the underlying is the same since CachingPool wraps
        underlyingService(1) returns Future.value(2)
        service2()(1)() mustEqual 2
      }
    }

    "flush the queue on close()" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
        val underlyingService = mock[Service[Any, Any]]
        underlyingService(Matchers.any) returns Future.value(obj)
        underlying() returns Future.value(underlyingService)
        underlyingService.isAvailable returns true

        val service = cachingPool()()
        service.release()
        there was no(underlyingService).release()

        cachingPool.close()
        there was one(underlyingService).release()
      }
    }

    "release services as they are released after close()" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
        val underlyingService = mock[Service[Any, Any]]
        underlyingService(Matchers.any) returns Future.value(obj)
        underlying() returns Future.value(underlyingService)
        underlyingService.isAvailable returns true

        val service = cachingPool()()
        cachingPool.close()
        there was no(underlyingService).release()
        service.release()
        there was one(underlyingService).release()
      }
    }

    "close the underlying factory" in {
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      cachingPool.close()
      there was one(underlying).close()
    }
  }
}
