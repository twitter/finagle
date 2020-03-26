package com.twitter.finagle.factory

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util._
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.StampedLock
import scala.collection.JavaConverters._

/**
 * A service factory that keeps track of idling times to implement
 * cache eviction.
 */
private class IdlingFactory[Req, Rep](self: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy[Req, Rep](self) {
  @volatile private[this] var watch = Stopwatch.start()
  private[this] val n = new AtomicInteger(0)

  private[this] val svcProxyFn: Try[Service[Req, Rep]] => Future[Service[Req, Rep]] = {
    case Throw(exc) =>
      decr()
      Future.exception(exc)

    case Return(service) =>
      Future.value(new ServiceProxy(service) {
        override def close(deadline: Time): Future[Unit] = {
          decr()
          super.close(deadline)
        }
      })
  }

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    n.getAndIncrement()
    self(conn).transform(svcProxyFn)
  }

  @inline private[this] def decr(): Unit = {
    if (n.decrementAndGet() == 0)
      watch = Stopwatch.start()
  }

  /**
   * Returns the duration of time for which this factory has been
   * idle--i.e. has no outstanding services.
   *
   * '''Bug:''' There is a small race here between checking n.get and
   * reading from the watch. (I.e. the factory can become nonidle
   * between the checks). This is fine.
   */
  def idleFor: Duration = if (n.get > 0) Duration.Zero else watch()
}

/**
 * A "read-through" cache of service factories.
 *
 * Eviction is based on cache size and idle time:
 *
 * 1. When the cache is full, a miss evicts the most idle factory.
 * When no underlying factories are idle, a one-shot factory is created.
 * This doesn't necessarily guarantee good performance: one-shots could
 * be created constantly for a hot cache key, but should work well
 * when there are a few hot keys.
 *
 * 2. Periodically evict factories that are idle for at least one TTI
 * (time-to-idle) period. An idle factory could remain in the cache
 * for up to (TTI * 2) minutes, with the caveat that we never expire
 * the last, least-idle entry.
 */
class ServiceFactoryCache[Key, Req, Rep](
  newFactory: Key => ServiceFactory[Req, Rep],
  timer: Timer,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  maxCacheSize: Int = 8,
  tti: Duration = 10.minutes)
    extends Closable {
  assert(maxCacheSize > 0)

  private[this] val cache = new util.HashMap[Key, IdlingFactory[Req, Rep]]()

  private[this] val lock = new StampedLock()

  private[this] val nmiss = statsReceiver.counter("misses")
  private[this] val nevict = statsReceiver.counter("evicts")
  private[this] val nexpires = statsReceiver.counter("expires")
  private[this] val noneshot = statsReceiver.counter("oneshots")

  /**
   * Evict entries that have been idle for longer than `tti`. If this
   * would empty the cache, instead evict all but the least idle entry.
   */
  private[this] def expireIdle(): Unit = {
    val writeStamp = lock.writeLock()
    try {
      val expired = cache.asScala.filter { case (_, fac) => fac.idleFor > tti }
      if (expired.nonEmpty) {
        val evictees = if (expired.size == cache.size) {
          expired - expired.minBy { case (_, fac) => fac.idleFor }._1
        } else {
          expired
        }

        evictees.foreach {
          case (key, _) =>
            val removed = cache.remove(key)
            removed.close()
        }
        nexpires.incr(evictees.size)
      }
    } finally {
      lock.unlockWrite(writeStamp)
    }
  }

  private[this] val expiryTask: Closable = {
    timer.schedule(tti) { expireIdle() }
  }

  /*
   * This returns a Service rather than a ServiceFactory to avoid
   * complicated bookkeeping around closing ServiceFactories. They can
   * be safely closed when evicted from the cache, when the entire
   * cache is closed, or in the case of one-shot services when the
   * service is closed; in all cases there are no references outside
   * of ServiceFactoryCache.
   */
  def apply(key: Key, conn: ClientConnection): Future[Service[Req, Rep]] = {
    val readStamp = lock.readLock()
    try {
      val svcFac = cache.get(key)
      if (svcFac != null)
        return svcFac(conn)
    } finally {
      lock.unlockRead(readStamp)
    }

    miss(key, conn)
  }

  private[this] def miss(key: Key, conn: ClientConnection): Future[Service[Req, Rep]] = {
    val writeStamp = lock.writeLock()

    val svcFac = cache.get(key)
    if (svcFac != null) {
      val readStamp = lock.tryConvertToReadLock(writeStamp)
      try {
        svcFac(conn)
      } finally {
        lock.unlockRead(readStamp)
      }
    } else {
      try {
        nmiss.incr()

        val factory = new IdlingFactory(newFactory(key))

        if (cache.size < maxCacheSize) {
          cache.put(key, factory)
          factory(conn)
        } else {
          findEvictee match {
            case Some(evicted) =>
              nevict.incr()
              val removed = cache.remove(evicted)
              removed.close()
              cache.put(key, factory)
              factory(conn)
            case None =>
              noneshot.incr()
              oneshot(factory, conn)
          }
        }
      } finally {
        lock.unlockWrite(writeStamp)
      }
    }
  }

  private[this] def oneshot(
    factory: ServiceFactory[Req, Rep],
    conn: ClientConnection
  ): Future[Service[Req, Rep]] =
    factory(conn).map { service =>
      new ServiceProxy(service) {
        override def close(deadline: Time): Future[Unit] =
          super.close(deadline).transform { _ => factory.close(deadline) }
      }
    }

  /** Must be called with the "write lock" held. */
  private[this] def findEvictee: Option[Key] = {
    var maxIdleFor = Duration.Bottom
    var maxKey: Any = null

    val iter = cache.entrySet.iterator
    while (iter.hasNext) {
      val entry = iter.next()
      val idleFor = entry.getValue.idleFor
      if (idleFor > maxIdleFor) {
        maxIdleFor = idleFor
        maxKey = entry.getKey
      }
    }
    if (maxIdleFor > Duration.Zero) Some(maxKey.asInstanceOf[Key])
    else None
  }

  def close(deadline: Time): Future[Unit] = {
    val writeStamp = lock.writeLock()
    val svcFacs =
      try {
        val values = List.newBuilder[Closable]
        val it = cache.values.iterator
        // Clear the cache to avoid racing with the timer task. If the
        // task is invoked after releasing this lock but before it has
        // a chance to close, it will be a noop.
        while (it.hasNext) {
          values += it.next()
          it.remove()
        }
        values
      } finally {
        lock.unlockWrite(writeStamp)
      }
    svcFacs += expiryTask
    Closable.all(svcFacs.result: _*).close(deadline)
  }

  private[finagle] def status(key: Key): Status = {
    val readStamp = lock.readLock()
    try {
      val svcFac = cache.get(key)
      if (svcFac != null)
        return svcFac.status
    } finally {
      lock.unlockRead(readStamp)
    }

    // This is somewhat dubious, as the status is outdated
    // pretty much right after we query it.
    val factory = newFactory(key)
    val status = factory.status
    factory.close()
    status
  }
}
