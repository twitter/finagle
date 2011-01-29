package com.twitter.finagle.pool

import collection.mutable.Queue

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import com.twitter.util
import com.twitter.util.{Time, Duration, Future}
import com.twitter.conversions.time._

import com.twitter.finagle.{Service, ServiceFactory}

object CachingPoolSpec extends Specification with Mockito {
  "CachingPool" should {
    val timer = new util.Timer {
      val scheduled = Queue[(Time, Function0[Unit])]()
      def schedule(when: Time)(f: => Unit) = {
        scheduled += (when, () => f)
        null  // not used by caller in this case.
      }
 
      def schedule(when: Time, period: Duration)(f: => Unit) =
        throw new Exception("periodic scheduling not supported")
 
      def stop() = ()
    }
 
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
        timer.scheduled must beEmpty

        f.release()
        there was one(underlyingService).isAvailable
        there was no(underlyingService).release()
        timer.scheduled must haveSize(1)
        val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
        timeToSchedule must be_==(Time.now + 5.seconds)
 
        // Reap!
        timeControl.advance(5.seconds)
        funToSchedule()
        there was one(underlyingService).release()
 
        timer.scheduled must beEmpty
      }
    }
 
    "reuse cached objects & revive from death row" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
        cachingPool.make()().release()

        there was one(underlying).make()
        there was no(underlyingService).release()
        timer.scheduled must haveSize(1)

        timeControl.advance(4.seconds)

        cachingPool.make()().release()
        there was one(underlying).make()
        there was no(underlyingService).release()
        timer.scheduled must haveSize(1)
 
        // Originally scheduled time.
        timeControl.advance(1.second)
 
        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now)
          funToSchedule()
        }
 
        timer.scheduled must haveSize(1)  // reschedule
        there was no(underlyingService).release()
 
        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now + 4.seconds)
          timeControl.advance(5.seconds)
          funToSchedule()
        }
 
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
     
        timer.scheduled must haveSize(1)
        ss foreach { s => there was no(s).release() }
        timeControl.advance(2.seconds)
     
        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now)
          funToSchedule()
        }
 
        there was one(s0).release()
 
        timeControl.advance(1.second)
        timer.scheduled must haveSize(1)
 
        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now)
          funToSchedule()
        }
 
        timer.scheduled must haveSize(1)
        timer.scheduled.head._1 must be_==(Time.now + 1.second)
 
        // Take it!
        cachingPool.make()()(123)() must be_==(o2)

        timeControl.advance(1.second)
 
        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now)
          funToSchedule()
        }
 
        // Nothing left.
        timer.scheduled must beEmpty
      }
    }
 
    "restart timers when a dispose occurs" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val underlyingService = mock[Service[Any, Any]]
        underlyingService.isAvailable returns true
        underlyingService(Matchers.any) returns Future.value(obj)

        val cachingPool = new CachingPool[Any, Any](underlying, 5.seconds, timer)
        underlying.make() returns Future.value(underlyingService)

        timer.scheduled must beEmpty
        val service = cachingPool.make()()
        service(123)() must be_==(obj)
        timer.scheduled must beEmpty

        service.release()
        timer.scheduled must haveSize(1)
        there was no(underlyingService).release()

        timer.scheduled.head._1 must be_==(Time.now + 5.seconds)

        timeControl.advance(1.second)

        cachingPool.make()()(123)() must be_==(obj)

        timeControl.advance(4.seconds)
        timer.scheduled must haveSize(1)

        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now)
          funToSchedule()
        }
 
        there was no(underlyingService).release()
        timer.scheduled must beEmpty

        service.release()

        there was no(underlyingService).release()
        timer.scheduled must haveSize(1)
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
        timer.scheduled must beEmpty
      }
    }
  }
}
