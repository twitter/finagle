package com.twitter.finagle.pool

import collection.mutable.Queue

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import com.twitter.util
import com.twitter.util.{Time, Duration, Future}
import com.twitter.conversions.time._

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.MockTimer

object CachingPoolSpec extends Specification with Mockito {
  "CachingPool" should {
    val timer = new MockTimer
    val obj = mock[Object]
    val underlying = mock[ServiceFactory[Any, Any]]
    val underlyingService = mock[Service[Any, Any]]
    underlyingService.isAvailable returns true
    underlyingService(Matchers.any) returns Future.value(obj)
    underlying.make() returns Future.value(underlyingService)

    "cache objects for the specified amount of time" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)

        val f = cachingPool.make()()
        f(123)() must be_==(obj)
        there was one(underlying).make()
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
        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
        cachingPool.make()().release()

        there was one(underlying).make()
        there was no(underlyingService).release()
        timer.tasks must haveSize(1)

        timeControl.advance(4.seconds)

        cachingPool.make()().release()
        there was one(underlying).make()
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

        val s0 = mock[Service[Any, Any]]; s0(Matchers.any) returns Future.value(o0)
        val s1 = mock[Service[Any, Any]]; s1(Matchers.any) returns Future.value(o1)
        val s2 = mock[Service[Any, Any]]; s2(Matchers.any) returns Future.value(o2)

        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
        underlying.make() returns Future.value(s0)
        val f0 = cachingPool.make()()
        f0(123)() must be_==(o0)

        underlying.make() returns Future.value(s1)
        val f1 = cachingPool.make()()
        f1(123)() must be_==(o1)

        underlying.make() returns Future.value(s2)
        val f2 = cachingPool.make()()
        f2(123)() must be_==(o2)
     
        val ss = Seq(s0, s1, s2)
        val fs = Seq(f0, f1, f2)

        there were three(underlying).make()
 
        ss foreach { _.isAvailable returns true }

        fs foreach { f =>
          f.release()
          timeControl.advance(1.second)
        }
     
        timer.tasks must haveSize(1)
        ss foreach { s => there was no(s).release() }
        timeControl.advance(2.seconds)
     
        timer.tick()

        there was one(s0).release()
 
        timeControl.advance(1.second)
        timer.tasks must haveSize(1)
 
        timer.tick()
 
        timer.tasks must haveSize(1)
        timer.tasks.head.when must be_==(Time.now + 1.second)
 
        // Take it!
        cachingPool.make()()(123)() must be_==(o2)

        timeControl.advance(1.second)
 
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

        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
        underlying.make() returns Future.value(underlyingService)

        timer.tasks must beEmpty
        val service = cachingPool.make()()
        service(123)() must be_==(obj)
        timer.tasks must beEmpty

        service.release()
        timer.tasks must haveSize(1)
        there was no(underlyingService).release()

        timer.tasks.head.when must be_==(Time.now + 5.seconds)

        timeControl.advance(1.second)

        cachingPool.make()()(123)() must be_==(obj)

        timeControl.advance(4.seconds)
        timer.tasks must haveSize(1)

        timer.tick()
        
        there was no(underlyingService).release()
        timer.tasks must beEmpty

        service.release()

        there was no(underlyingService).release()
        timer.tasks must haveSize(1)
      }
    }
 
    "don't cache unhealthy objects" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
        val underlyingService = mock[Service[Any, Any]]
        underlyingService(Matchers.any) returns Future.value(obj)
        underlying.make() returns Future.value(underlyingService)
        underlyingService.isAvailable returns false
 
        val service = cachingPool.make()()
        service(123)() must be_==(obj)
        
        service.release()
        there was one(underlyingService).isAvailable
        there was one(underlyingService).release()

        // No need to clean up an already disposed object. 
        timer.tasks must beEmpty
      }
    }

    "flush the queue on close()" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
        val underlyingService = mock[Service[Any, Any]]
        underlyingService(Matchers.any) returns Future.value(obj)
        underlying.make() returns Future.value(underlyingService)
        underlyingService.isAvailable returns true

        val service = cachingPool.make()()
        service.release()
        there was no(underlyingService).release()

        cachingPool.close()
        there was one(underlyingService).release()
      }    
    }

    "release services as they are released after close()" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
        val underlyingService = mock[Service[Any, Any]]
        underlyingService(Matchers.any) returns Future.value(obj)
        underlying.make() returns Future.value(underlyingService)
        underlyingService.isAvailable returns true

        val service = cachingPool.make()()
        cachingPool.close()
        there was no(underlyingService).release()
        service.release()
        there was one(underlyingService).release()
      }    
    }

    "close the underlying factory" in {
      val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
      cachingPool.close()
      there was one(underlying).close()
    }
  }
}
