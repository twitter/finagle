package com.twitter.finagle.pool

import collection.mutable.Queue

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import com.twitter.util
import com.twitter.util.{Time, Duration, Future}
import com.twitter.conversions.time._

object LifeCycleFactorySpec extends Specification with Mockito {
  "LifeCycleFactory" should {
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
    val underlying = mock[LifecycleFactory[Any]]
    underlying.isHealthy(obj) returns true

    "cache objects for the specified amount of time" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val clf = new CachingLifecycleFactory[Any](underlying, 5.seconds, timer)
        underlying.make() returns Future.value(obj)
         
        val f = clf.make()
        f.isDefined must beTrue
        f() must be_==(obj)
        there was one(underlying).make()
        timer.scheduled must beEmpty
         
        clf.dispose(f())
        there was one(underlying).isHealthy(obj)
        there was no(underlying).dispose(obj)
        timer.scheduled must haveSize(1)
        val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
        timeToSchedule must be_==(Time.now + 5.seconds)

        // Reap!
        timeControl.advance(5.seconds)
        funToSchedule()
        there was one(underlying).dispose(obj)

        timer.scheduled must beEmpty
      }
    }

    "reuse cached objects & revive from death row" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val clf = new CachingLifecycleFactory[Any](underlying, 5.seconds, timer)
        underlying.make() returns Future.value(obj)

        clf.dispose(clf.make()())
        there was one(underlying).make()
        there was no(underlying).dispose(obj)
        timer.scheduled must haveSize(1)

        timeControl.advance(4.seconds)

        clf.dispose(clf.make()())
        there was one(underlying).make()
        there was no(underlying).dispose(obj)
        timer.scheduled must haveSize(1)

        // Originally scheduled time.
        timeControl.advance(1.second)

        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now)
          funToSchedule()
        }

        timer.scheduled must haveSize(1)  // reschedule
        there was no(underlying).dispose()

        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now + 4.seconds)
          timeControl.advance(5.seconds)
          funToSchedule()
        }

        there was one(underlying).dispose(obj)
      }
    }

    "handle multiple objects, expiring them only after they are due to" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val o0 = mock[Object]
        val o1 = mock[Object]
        val o2 = mock[Object]
     
        val clf = new CachingLifecycleFactory[Any](underlying, 5.seconds, timer)
        underlying.make() returns Future.value(o0)
        clf.make()() must be_==(o0)
        underlying.make() returns Future.value(o1)
        clf.make()() must be_==(o1)
        underlying.make() returns Future.value(o2)
        clf.make()() must be_==(o2)
     
        there were three(underlying).make()

        underlying.isHealthy(Matchers.any) returns true

        clf.dispose(o0)
        timeControl.advance(1.second)
        clf.dispose(o1)
        timeControl.advance(1.second)
        clf.dispose(o2)
        timeControl.advance(1.second)
     
        timer.scheduled must haveSize(1)
        there was no(underlying).dispose(Matchers.anyObject)
        timeControl.advance(2.seconds)
     
        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now)
          funToSchedule()
        }

        there was one(underlying).dispose(o0)

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
        clf.make()() must be_==(o2)

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
        val clf = new CachingLifecycleFactory[Any](underlying, 5.seconds, timer)
        underlying.make() returns Future.value(obj)

        timer.scheduled must beEmpty
        clf.make()() must be_==(obj)
        timer.scheduled must beEmpty
                                  
        clf.dispose(obj)
        timer.scheduled must haveSize(1)
        there was no(underlying).dispose(obj)

        timer.scheduled.head._1 must be_==(Time.now + 5.seconds)

        timeControl.advance(1.second)

        clf.make()() must be_==(obj)

        timeControl.advance(4.seconds)
        timer.scheduled must haveSize(1)

        {
          val (timeToSchedule, funToSchedule) = timer.scheduled.dequeue()
          timeToSchedule must be_==(Time.now)
          funToSchedule()
        }

        there was no(underlying).dispose(obj)
        timer.scheduled must beEmpty

        clf.dispose(obj)
                                  
        there was no(underlying).dispose(obj)
        timer.scheduled must haveSize(1)
      }
    }

    "don't cache unhealthy objects" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val clf = new CachingLifecycleFactory[Any](underlying, 5.seconds, timer)
        underlying.make() returns Future.value(obj)
        underlying.isHealthy(obj) returns false

        clf.make()() must be_==(obj)
        
        clf.dispose(obj)
        there was one(underlying).isHealthy(obj)
        there was one(underlying).dispose(obj)

        // No need to clean up an already disposed object. 
        timer.scheduled must beEmpty
      }
    }
  }
}
