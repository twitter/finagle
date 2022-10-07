package com.twitter.finagle.pool

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Status
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.MockTimer
import com.twitter.util.Time
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class CachingPoolTest extends AnyFunSuite with MockitoSugar with OneInstancePerTest {

  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  private val timer = new MockTimer
  private val obj = mock[Object]
  private val underlying = mock[ServiceFactory[Any, Any]]
  when(underlying.close(any[Time])).thenReturn(Future.Done)
  private val underlyingService = mock[Service[Any, Any]]
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

      val f = await(cachingPool())
      assert(await(f(123)) == obj)
      verify(underlying)()

      f.close()
      verify(underlyingService).status
      verify(underlyingService, never()).close(any[Time])

      // Reap!
      timeControl.advance(5.seconds)
      timer.tick()
      verify(underlyingService).close(any[Time])
    }
  }

  test("insert an instance into the cache a single time even when closed multiple times") {
    val sr = new InMemoryStatsReceiver
    def poolsize() = sr.gauges(Seq("pool_cached"))()
    val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, Duration.Top, timer, sr)
    val svc1 = await(cachingPool())
    await(svc1.close())
    assert(poolsize() == 1)

    await(svc1.close())
    // not two!
    assert(poolsize() == 1)
  }

  test("reuse cached objects until after their ttl") {
    Time.withCurrentTimeFrozen { timeControl =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      await(cachingPool()).close()

      verify(underlying)()
      verify(underlyingService, never()).close(any[Time])

      timeControl.advance(4.seconds)

      await(cachingPool()).close()
      verify(underlying)()
      verify(underlyingService, never()).close(any[Time])

      // Originally scheduled time.
      timeControl.advance(1.second)
      timer.tick()

      verify(underlyingService, never()).close(any[Time])

      timeControl.advance(5.seconds)
      timer.tick()

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
      val f0 = await(cachingPool())
      assert(await(f0(123)) == o0)

      when(underlying()).thenReturn(Future.value(s1))
      val f1 = await(cachingPool())
      assert(await(f1(123)) == o1)

      when(underlying()).thenReturn(Future.value(s2))
      val f2 = await(cachingPool())
      assert(await(f2(123)) == o2)

      val ss = Seq(s0, s1, s2)
      val fs = Seq(f0, f1, f2)

      verify(underlying, times(3))()

      ss.foreach { s => when(s.status).thenReturn(Status.Open) }

      fs.foreach { f =>
        timeControl.advance(5.second)
        f.close()
      }

      ss.foreach { s => verify(s, never()).close(any[Time]) }

      timer.tick()

      verify(s0).close(any[Time])
      verify(s1).close(any[Time])
      verify(s2, never()).close(any[Time])

      timer.tick()

      // Take it!
      assert(await(await(cachingPool())(123)) == o2)
    }
  }

  test("don't cache unhealthy objects") {
    Time.withCurrentTimeFrozen { _ =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      val underlyingService = mock[Service[Any, Any]]
      when(underlyingService.close(any[Time])).thenReturn(Future.Done)
      when(underlyingService(any[Any])).thenReturn(Future.value(obj))
      when(underlying()).thenReturn(Future.value(underlyingService))
      when(underlyingService.status).thenReturn(Status.Closed)

      val service = await(cachingPool())
      assert(await(service(123)) == obj)

      service.close()
      verify(underlyingService).status
      verify(underlyingService).close(any[Time])
    }
  }

  test("flush the queue on close()") {
    Time.withCurrentTimeFrozen { _ =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      val underlyingService = mock[Service[Any, Any]]
      when(underlyingService.close(any[Time])).thenReturn(Future.Done)
      when(underlyingService(any[Time])).thenReturn(Future.value(obj))
      when(underlying()).thenReturn(Future.value(underlyingService))
      when(underlyingService.status).thenReturn(Status.Open)

      val service = await(cachingPool())
      service.close()
      verify(underlyingService, never()).close(any[Time])

      cachingPool.close()
      verify(underlyingService).close(any[Time])
    }
  }

  test("release services as they are released after close()") {
    Time.withCurrentTimeFrozen { _ =>
      val cachingPool = new CachingPool[Any, Any](underlying, Int.MaxValue, 5.seconds, timer)
      val underlyingService = mock[Service[Any, Any]]
      when(underlyingService.close(any[Time])).thenReturn(Future.Done)
      when(underlyingService(any[Any])).thenReturn(Future.value(obj))
      when(underlying()).thenReturn(Future.value(underlyingService))
      when(underlyingService.status).thenReturn(Status.Open)

      val service = await(cachingPool())
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

  test("service is closed when pool is full on completion") {
    val service1 = mock[Service[Any, Any]]
    when(service1.status).thenReturn(Status.Open)
    when(service1.close(any())).thenReturn(Future.Done)

    val service2 = mock[Service[Any, Any]]
    when(service2.status).thenReturn(Status.Open)
    when(service2.close(any())).thenReturn(Future.Done)

    val factory = mock[ServiceFactory[Any, Any]]
    when(factory())
      .thenReturn(Future.value(service1))
      .thenReturn(Future.value(service2))

    val poolSize = 1
    val pool = new CachingPool[Any, Any](factory, poolSize, 5.seconds, timer)

    // take out 1 more than the pool size
    val s1 = await(pool())
    val s2 = await(pool())

    // the first service can go back into the pool as there is room for it.
    await(s1.close())
    verify(service1, never()).close(any())

    // the second one cannot go back into the poll, as it is already at
    // capacity, and thus must close it.
    await(s2.close())
    verify(service2).close(any())
  }

}
