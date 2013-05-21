package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.MockTimer
import com.twitter.finagle.stats.{Counter, StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.Service
import com.twitter.util.{Future, Time, Promise, Return, Duration, Timer}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ExpiringServiceSpec extends SpecificationWithJUnit with Mockito {
  "ExpiringService" should {
    val stats = mock[StatsReceiver]
    val idleCounter = mock[Counter]
    val lifeCounter = mock[Counter]
    stats.counter("idle") returns idleCounter
    stats.counter("lifetime") returns lifeCounter

    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      val underlying = mock[Service[Any, Any]]
      underlying.close(any) returns Future.Done
      val promise = new Promise[Int]
      underlying(123) returns promise
      underlying.isAvailable returns true

      class ReleasingExpiringService[Req, Rep](
          self: Service[Req, Rep],
          maxIdleTime: Option[Duration],
          maxLifeTime: Option[Duration],
          timer: Timer,
          stats: StatsReceiver)
      extends ExpiringService[Req, Rep](
          self, maxIdleTime, maxLifeTime,
          timer, stats)
      {
        def onExpire() { self.close() }
      }

      "releasing" in {
        val service = new ReleasingExpiringService[Any, Any](
          underlying, Some(10.seconds), None, timer, NullStatsReceiver)
        there was no(underlying).close(any)

        "cancel timers on release" in {
          val count = timer.tasks.size
          val service = new ReleasingExpiringService[Any, Any](
            underlying, Some(10.seconds), Some(5.seconds), timer, NullStatsReceiver)
          timer.tasks.size mustEqual count + 2
          service.close()
          timer.tasks.size mustEqual count
        }

        "after expiring" in {
          timeControl.advance(10.seconds)
          timer.tick()

          there was one(underlying).close(any)

          // Now attempt to release it once more:
          service.close()
          there was one(underlying).close(any)
        }
      }

      "idle time between requests" in {
        val service = new ReleasingExpiringService[Any, Any](underlying, Some(10.seconds), None, timer, NullStatsReceiver)
        timer.tasks must haveSize(1)
        "expire after the given idle time" in {
          // For some reason, this complains of different types:
          //   timer.tasks.head.when must be_==(Time.now + 10.seconds)

          timeControl.advance(10.seconds)
          timer.tick()

          there was one(underlying).close(any)

          timer.tasks must beEmpty
        }

        "increments the counter when the timer fires" in {
          val service = new ReleasingExpiringService[Any, Any](
            underlying, Some(10.seconds), None, timer, stats)
          timeControl.advance(10.seconds)
          timer.tick()
          there was one(idleCounter).incr()
          there was no(lifeCounter).incr()
        }

        "cancel the timer when a request is issued" in {
          service(123)
          timer.tasks must beEmpty
        }

        "restart the timer when the request finished" in {
          service(123)
          timer.tasks must beEmpty

          timeControl.advance(10.seconds)
          timer.tick()

          timer.tasks must beEmpty
          promise() = Return(321)
          timer.tasks must haveSize(1)

          there was no(underlying).close(any)
          timeControl.advance(10.seconds)
          timer.tick()

          there was one(underlying).close(any)
        }
      }

      "life time of a connection" in {
        val service = new ReleasingExpiringService[Any, Any](
          underlying, None, Some(10.seconds), timer, NullStatsReceiver)
        timer.tasks must haveSize(1)
        "expire after the given idle time" in {
          // For some reason, this complains of different types:
          //   timer.tasks.head.when must be_==(Time.now + 10.seconds)

          timeControl.advance(10.seconds)
          timer.tick()

          there was one(underlying).close(any)

          timer.tasks must beEmpty
        }

        "does not cancel the timer when a request is issued" in {
          service(123)
          timer.tasks must haveSize(1)
          timer.tasks.head.isCancelled must beFalse
        }
      }

      "idle timer fires before life timer fires" in {
        val service = new ReleasingExpiringService[Any, Any](
          underlying, Some(10.seconds), Some(1.minute), timer, NullStatsReceiver)
        timer.tasks must haveSize(2)

        "expire after the given idle time" in {
          // For some reason, this complains of different types:
          //   timer.tasks.head.when must be_==(Time.now + 10.seconds)

          timeControl.advance(10.seconds)
          timer.tick()

          there was one(underlying).close(any)

          timer.tasks must beEmpty
        }
      }

      "life timer fires before idle timer fires" in {
        val service = new ReleasingExpiringService[Any, Any](
          underlying, Some(10.seconds), Some(15.seconds), timer, stats)
        timer.tasks must haveSize(2)
        timer.tasks forall(!_.isCancelled) must beTrue

        "expire after the given life time" in {
          service(123)
          timer.tasks must haveSize(1)
          timer.tasks.head.isCancelled must beFalse

          timeControl.advance(8.seconds)
          timer.tick()

          timer.tasks must haveSize(1)
          timer.tasks.head.isCancelled must beFalse

          promise() = Return(321)
          timer.tasks must haveSize(2)
          timer.tasks forall(!_.isCancelled) must beTrue

          there was no(underlying).close(any)
          timeControl.advance(8.seconds)
          timer.tick()

          timer.tasks must beEmpty
          there was one(underlying).close(any)
        }
      }

      "increments the counter when the timer fires" in {
        val service = new ReleasingExpiringService[Any, Any](underlying, None, Some(10.seconds), timer, stats)
        timeControl.advance(10.seconds)
        timer.tick()
        there was one(lifeCounter).incr()
        there was no(idleCounter).incr()
      }
    }
  }
}
