package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{Counter, StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{Service, Status}
import com.twitter.util.{Future, Time, MockTimer, Promise, Return, Duration, Timer}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ExpiringServiceTest extends FunSuite with MockitoSugar {

  val frozenNow = Time.now

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

  def newCtx() = new {
    val stats = mock[StatsReceiver]
    val idleCounter = mock[Counter]
    val lifeCounter = mock[Counter]
    when(stats.counter("idle")).thenReturn(idleCounter)
    when(stats.counter("lifetime")).thenReturn(lifeCounter)

    val timer = new MockTimer
    val underlying = mock[Service[Any, Any]]
    when(underlying.close(any[Time])).thenReturn(Future.Done)
    val promise = new Promise[Int]
    when(underlying(123)).thenReturn(promise)
    when(underlying.status).thenReturn(Status.Open)
  }

  test("cancelling timers on release") {
    Time.withTimeAt(frozenNow) { _ =>
      val ctx = newCtx()
      import ctx._

      val count = timer.tasks.size
      val service = new ReleasingExpiringService[Any, Any](
        underlying, Some(10.seconds), Some(5.seconds), timer, NullStatsReceiver)
      assert(timer.tasks.size == count + 2)
      service.close()
      assert(timer.tasks.size == count)
    }
  }

  test("closing a service after expiring") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._
      val service = new ReleasingExpiringService[Any, Any](underlying, Some(10.seconds), None, timer, NullStatsReceiver)
      verify(underlying, never()).close(any[Time])

      timeControl.advance(10.seconds)
      timer.tick()

      verify(underlying).close(any[Time])

      // Now attempt to release it once more:
      service.close()
      verify(underlying).close(any[Time])
    }
  }

  test("expiring after the given idle time") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._
      val service = new ReleasingExpiringService[Any, Any](underlying, Some(10.seconds), None, timer, NullStatsReceiver)
      assert(timer.tasks.size == 1)
      assert(timer.tasks.head.when == Time.now + 10.seconds)

      timeControl.advance(10.seconds)
      timer.tick()

      verify(underlying).close(any[Time])

      assert(timer.tasks.isEmpty)
    }
  }

  test("incrementing the counter when the timer fires") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._

      val service = new ReleasingExpiringService[Any, Any](underlying, Some(10.seconds), None, timer, stats)
      timeControl.advance(10.seconds)
      timer.tick()
      verify(idleCounter).incr()
      verify(lifeCounter, never()).incr()
    }
  }

  test("cancelling the timer when a request is issued") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._
      val service = new ReleasingExpiringService[Any, Any](underlying, Some(10.seconds), None, timer, NullStatsReceiver)
      assert(timer.tasks.size == 1)

      service(123)
      assert(timer.tasks.isEmpty)
    }
  }

  test("restarting the timer when the request finishes") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._
      val service = new ReleasingExpiringService[Any, Any](underlying, Some(10.seconds), None, timer, NullStatsReceiver)
      assert(timer.tasks.size == 1)

      service(123)
      assert(timer.tasks.isEmpty)

      timeControl.advance(10.seconds)
      timer.tick()

      assert(timer.tasks.isEmpty)
      promise() = Return(321)
      assert(timer.tasks.size == 1)

      verify(underlying, never()).close(any[Time])
      timeControl.advance(10.seconds)
      timer.tick()

      verify(underlying).close(any[Time])
    }
  }

  test("expiring life time after the given idle time") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._
      val service = new ReleasingExpiringService[Any, Any](underlying, None, Some(10.seconds), timer, NullStatsReceiver)
      assert(timer.tasks.size == 1)
      assert(timer.tasks.head.when == Time.now + 10.seconds)

      timeControl.advance(10.seconds)
      timer.tick()

      verify(underlying).close(any[Time])

      assert(timer.tasks.isEmpty)
    }
  }

  test("not cancelling the timer when a request is issued") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._
      val service = new ReleasingExpiringService[Any, Any](underlying, None, Some(10.seconds), timer, NullStatsReceiver)
      assert(timer.tasks.size == 1)

      service(123)
      assert(timer.tasks.size == 1)
      assert(!timer.tasks.head.isCancelled)
    }
  }

  test("idle timer fires before the life timer fires") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._

      val service = new ReleasingExpiringService[Any, Any](
        underlying, Some(10.seconds), Some(1.minute), timer, NullStatsReceiver)
      assert(timer.tasks.size == 2)
      assert(timer.tasks.head.when == Time.now + 10.seconds)

      timeControl.advance(10.seconds)
      timer.tick()

      verify(underlying).close(any[Time])

      assert(timer.tasks.isEmpty)
    }
  }

  test("expiring after the given life time") {
    Time.withTimeAt(frozenNow) { timeControl =>
      val ctx = newCtx()
      import ctx._
      val service = new ReleasingExpiringService[Any, Any](underlying, Some(10.seconds), Some(15.seconds), timer, stats)
      assert(timer.tasks.size == 2)
      assert(timer.tasks.forall(!_.isCancelled))

      service(123)
      assert(timer.tasks.size == 1)
      assert(timer.tasks.head.isCancelled == false)

      timeControl.advance(8.seconds)
      timer.tick()

      assert(timer.tasks.size == 1)
      assert(timer.tasks.head.isCancelled == false)

      promise() = Return(321)
      assert(timer.tasks.size == 2)
      assert(timer.tasks.forall(!_.isCancelled))

      verify(underlying, never()).close(any[Time])
      timeControl.advance(8.seconds)
      timer.tick()

      assert(timer.tasks.isEmpty)
      verify(underlying).close(any[Time])
    }
  }
}
