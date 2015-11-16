package com.twitter.finagle.pool

import com.twitter.conversions.time._
import com.twitter.finagle.{Status, Service, ServiceFactory}
import com.twitter.util.{Await, Duration, MockTimer, Time, Future}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, OneInstancePerTest}

@RunWith(classOf[JUnitRunner])
class CachingPoolTest extends FunSuite with MockitoSugar with OneInstancePerTest {

  val timer = new MockTimer
  val obj = mock[Object]
  val underlying = mock[ServiceFactory[Any, Any]]
  when(underlying.close(any[Time])).thenReturn(Future.Done)
  val underlyingService = mock[Service[Any, Any]]
  when(underlyingService.close(any[Time])).thenReturn(Future.Done)
  when(underlyingService.status).thenReturn(Status.Open)
  when(underlyingService(any[Any])).thenReturn(Future.value(obj))
  when(underlying()).thenReturn(Future.value(underlyingService))


  test("reflect the underlying factory availability") {
    val pool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
    when(underlying.status).thenReturn(Status.Closed)
    assert(!pool.isAvailable)
    verify(underlying).status
    when(underlying.status).thenReturn(Status.Open)
    assert(pool.isAvailable)
    verify(underlying, times(2)).status
  }

  test("cache objects for the specified amount of time") {
    Time.withCurrentTimeFrozen { timeControl =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)

      val f = Await.result(cachingPool())
      assert(Await.result(f(123)) == obj)
      verify(underlying)()
      assert(timer.tasks.isEmpty)

      f.close()
      verify(underlyingService).status
      verify(underlyingService, never()).close(any[Time])
      assert(timer.tasks.size == 1)
      assert(timer.tasks.head.when == Time.now + 5.seconds)

      // Reap!
      timeControl.advance(5.seconds)
      timer.tick()
      verify(underlyingService).close(any[Time])

      assert(timer.tasks.isEmpty)
    }
  }

  test("do not schedule timer tasks if items never expire") {
    val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, Duration.Top, timer)

    val service = Await.result(cachingPool())
    assert(service == underlyingService)
    service.close()
    verify(underlyingService, never()).close(any[Time])
    assert(timer.tasks.isEmpty)
    assert(Await.result(cachingPool()) == underlyingService)
  }

  test("reuse cached objects & revive from death row") {
    Time.withCurrentTimeFrozen { timeControl =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      Await.result(cachingPool()).close()
      assert(timer.tasks.size == 1)

      verify(underlying)()
      verify(underlyingService, never()).close(any[Time])
      assert(timer.tasks.size == 1)

      timeControl.advance(4.seconds)

      Await.result(cachingPool()).close()
      verify(underlying)()
      verify(underlyingService, never()).close(any[Time])
      assert(timer.tasks.size == 1)

      // Originally scheduled time.
      timeControl.advance(1.second)
      timer.tick()

      assert(timer.tasks.size == 1) // reschedule
      verify(underlyingService, never()).close(any[Time])

      assert(timer.tasks.head.when == Time.now + 4.seconds)
      timeControl.advance(5.seconds)
      timer.tick()
      assert(timer.tasks.isEmpty)

      verify(underlyingService).close(any[Time])
    }
  }

  test("handle multiple objects, expiring them only after they are due to") {
    Time.withCurrentTimeFrozen { timeControl =>
      val o0 = mock[Object]
      val o1 = mock[Object]
      val o2 = mock[Object]

      val s0 = mock[Service[Any, Any]]
      when(s0(any[Any])).thenReturn(Future.value(o0))
      when(s0.close(any[Time])).thenReturn(Future.Done)
      val s1 = mock[Service[Any, Any]]
      when(s1(any[Any])).thenReturn(Future.value(o1))
      when(s1.close(any[Time])).thenReturn(Future.Done)
      val s2 = mock[Service[Any, Any]]
      when(s2(any[Any])).thenReturn(Future.value(o2))
      when(s2.close(any[Time])).thenReturn(Future.Done)

      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      when(underlying()).thenReturn(Future.value(s0))
      val f0 = Await.result(cachingPool())
      assert(Await.result(f0(123)) == o0)

      when(underlying()).thenReturn(Future.value(s1))
      val f1 = Await.result(cachingPool())
      assert(Await.result(f1(123)) == o1)

      when(underlying()).thenReturn(Future.value(s2))
      val f2 = Await.result(cachingPool())
      assert(Await.result(f2(123)) == o2)

      val ss = Seq(s0, s1, s2)
      val fs = Seq(f0, f1, f2)

      verify(underlying, times(3))()

      ss foreach { s => when(s.status).thenReturn(Status.Open) }

      fs foreach { f =>
        timeControl.advance(5.second)
        f.close()
      }

      assert(timer.tasks.size == 1)
      ss foreach { s => verify(s, never()).close(any[Time]) }

      timer.tick()

      verify(s0).close(any[Time])
      verify(s1).close(any[Time])
      verify(s2, never()).close(any[Time])

      assert(timer.tasks.size == 1)
      timer.tick()

      assert(timer.tasks.head.when == Time.now + 5.seconds)

      // Take it!
      assert(Await.result(Await.result(cachingPool())(123)) == o2)

      timeControl.advance(5.seconds)

      timer.tick()

      // Nothing left.
      assert(timer.tasks.isEmpty)
    }
  }

  test("restart timers when a dispose occurs") {
    Time.withCurrentTimeFrozen { timeControl =>
      val underlyingService = mock[Service[Any, Any]]
      when(underlyingService.close(any[Time])).thenReturn(Future.Done)
      when(underlyingService.status).thenReturn(Status.Open)
      when(underlyingService(any[Any])).thenReturn(Future.value(obj))

      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      when(underlying()).thenReturn(Future.value(underlyingService))

      assert(timer.tasks.isEmpty)
      val service = Await.result(cachingPool())
      assert(Await.result(service(123)) == obj)
      assert(timer.tasks.isEmpty)

      service.close()
      assert(timer.tasks.size == 1)
      verify(underlyingService, never()).close(any[Time])

      assert(timer.tasks.head.when == Time.now + 5.seconds)

      timeControl.advance(1.second)

      assert(Await.result(Await.result(cachingPool())(123)) == obj)

      timeControl.advance(4.seconds)
      assert(timer.tasks.isEmpty)

      timer.tick()

      verify(underlyingService, never()).close(any[Time])

      service.close()

      verify(underlyingService, never()).close(any[Time])
      assert(timer.tasks.size == 1)
    }
  }

  test("don't cache unhealthy objects") {
    Time.withCurrentTimeFrozen { timeControl =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      val underlyingService = mock[Service[Any, Any]]
      when(underlyingService.close(any[Time])).thenReturn(Future.Done)
      when(underlyingService(any[Any])).thenReturn(Future.value(obj))
      when(underlying()).thenReturn(Future.value(underlyingService))
      when(underlyingService.status).thenReturn(Status.Closed)

      val service = Await.result(cachingPool())
      assert(Await.result(service(123)) == obj)

      service.close()
      verify(underlyingService).status
      verify(underlyingService).close(any[Time])

      // No need to clean up an already disposed object.
      assert(timer.tasks.isEmpty)
    }
  }

  test("flush the queue on close()") {
    Time.withCurrentTimeFrozen { timeControl =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      val underlyingService = mock[Service[Any, Any]]
      when(underlyingService.close(any[Time])).thenReturn(Future.Done)
      when(underlyingService(any[Time])).thenReturn(Future.value(obj))
      when(underlying()).thenReturn(Future.value(underlyingService))
      when(underlyingService.status).thenReturn(Status.Open)

      val service = Await.result(cachingPool())
      service.close()
      verify(underlyingService, never()).close(any[Time])

      cachingPool.close()
      verify(underlyingService).close(any[Time])
    }
  }

  test("release services as they are released after close()") {
    Time.withCurrentTimeFrozen { timeControl =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      val underlyingService = mock[Service[Any, Any]]
      when(underlyingService.close(any[Time])).thenReturn(Future.Done)
      when(underlyingService(any[Any])).thenReturn(Future.value(obj))
      when(underlying()).thenReturn(Future.value(underlyingService))
      when(underlyingService.status).thenReturn(Status.Open)

      val service = Await.result(cachingPool())
      cachingPool.close()
      verify(underlyingService, never()).close(any[Time])
      service.close()
      verify(underlyingService).close(any[Time])
    }
  }

  test("close the underlying factory") {
    val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
    cachingPool.close()
    verify(underlying).close(any[Time])
  }

}
