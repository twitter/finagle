package com.twitter.finagle.factory

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.atomic.AtomicInteger
import com.twitter.finagle.{Service, ServiceFactory, ClientConnection, ServiceProxy, ServiceFactoryProxy}
import com.twitter.util.{Closable, Future, Stopwatch, Throw, Return, Time, Duration}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.Trace

/**
 * A service factory that keeps track of idling times to implement
 * cache eviction.
 */
private class IdlingFactory[Req, Rep](self: ServiceFactory[Req, Rep])
  extends ServiceFactoryProxy[Req, Rep](self) {
  @volatile private[this] var watch = Stopwatch.start()
  private[this] val n = new AtomicInteger(0)

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    n.getAndIncrement()

    self(conn) transform {
      case Throw(exc) =>
        decr()
        Future.exception(exc)

      case Return(service) =>
        Future.value(new ServiceProxy(service) {
          override def close(deadline: Time) = {
            decr()
            super.close(deadline)
          }
        })
    }
  }

  @inline private[this] def decr() {
    if (n.decrementAndGet() == 0)
      watch = Stopwatch.start()
  }

  /**
   * Returns the duration of time for which this factory has been
   * idle--i.e. has no outstanding services.
   *
   * @bug There is a small race here between checking n.get and
   * reading from the watch. (I.e. the factory can become nonidle
   * between the checks). This is fine.
   */
  def idleFor = if (n.get > 0) Duration.Zero else watch()
}

/**
 * A "read-through" cache of service factories. Eviction is based on
 * idle time -- when no underlying factories are idle, one-shot
 * factories are created. This doesn't necessarily guarantee good
 * performance: one-shots could be created constantly for a hot cache
 * key, but should work well when there are a few hot keys.
 */
private class ServiceFactoryCache[Key, Req, Rep](
    newFactory: Key => ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver = NullStatsReceiver,
    maxCacheSize: Int = 8)
  extends Closable {
  assert(maxCacheSize > 0)

  @volatile private[this] var cache =
    Map.empty: Map[Key, IdlingFactory[Req, Rep]]

  private[this] val (readLock, writeLock) = {
    val rw = new ReentrantReadWriteLock()
    (rw.readLock(), rw.writeLock())
  }

  private[this] val nmiss = statsReceiver.counter("misses")
  private[this] val nevict = statsReceiver.counter("evicts")
  private[this] val noneshot = statsReceiver.counter("oneshots")
  private[this] val nidle = statsReceiver.addGauge("idle") {
    cache count { case (_, f) => f.idleFor > Duration.Zero }
  }
  private[this] val misstime = statsReceiver.stat("misstime_ms")

  def apply(key: Key, conn: ClientConnection): Future[Service[Req, Rep]] = {
    readLock.lock()
    try {
      if (cache contains key)
        return cache(key).apply(conn)
    } finally {
      readLock.unlock()
    }

    miss(key, conn)
  }

  private[this] def miss(key: Key, conn: ClientConnection): Future[Service[Req, Rep]] = {
    writeLock.lock()

    if (cache contains key) {
      readLock.lock()
      writeLock.unlock()
      try {
        return cache(key).apply(conn)
      } finally {
        readLock.unlock()
      }
    }

    val watch = Stopwatch.start()

    val svc = try {
      nmiss.incr()

      val factory = new IdlingFactory(newFactory(key))

      if (cache.size < maxCacheSize) {
        cache += (key -> factory)
        cache(key).apply(conn)
      } else {
        findEvictee() match {
          case Some(evicted) =>
            nevict.incr()
            cache(evicted).close()
            cache = cache - evicted + (key -> factory)
            cache(key).apply(conn)
          case None =>
            noneshot.incr()
            oneshot(factory, conn)
        }
      }
    } finally {
      writeLock.unlock()
    }

    svc onSuccess { _ =>
      val d = watch()
        // generalize message
      Trace.record("Interpreter cache miss with key "+key+": "+d, d)
      misstime.add(d.inMilliseconds)
    }
  }

  private[this] def oneshot(factory: ServiceFactory[Req, Rep], conn: ClientConnection)
  : Future[Service[Req, Rep]] =
    factory(conn) map { service =>
      new ServiceProxy(service) {
        override def close(deadline: Time) =
          super.close(deadline) transform { case _ =>
            factory.close(deadline)
          }
      }
    }

  private[this] def findEvictee(): Option[Key] = {
    val (evictNamer, evictFactory) = cache maxBy { case (_, fac) => fac.idleFor }
    if (evictFactory.idleFor > Duration.Zero) Some(evictNamer)
    else None
  }

  def close(deadline: Time) = Closable.all(cache.values.toSeq:_*).close(deadline)
  def isAvailable = cache.isEmpty || cache.values.exists(_.isAvailable)
}
