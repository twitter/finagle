package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Time, Duration, Stopwatch, Var, Throw, Return, Closable}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

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

  // There is a small race here between checking n.get and reading
  // from the watch. (I.e. the factory can become nonidle between the
  // checks). This is fine.
  def idleFor = if (n.get > 0) Duration.Zero else watch()
}

/**
 * A factory that routes to the local interpretation of the passed-in
 * [[com.twitter.finagle.UninterpretedName UninterpretedName]]. It
 * calls `newFactory` to mint a new
 * [[com.twitter.finagle.ServiceFactory ServiceFactory]] for novel
 * name evaluations, caching up to `maxCacheSize` of them.
 *
 * When more than `maxCacheSize` factories have been cached,
 * ephemeral, "one-shot" factories are created.
 *
 * We cache name binding here due to the fact that Namers are scoped
 * to a request, meaning they may change for each invocation of
 * ServiceFactory#apply. While a name's resolution is dynamic, it
 * depends only on the Namer against which it is interpreted, thus
 * the Namer makes for a natural cache key.
 */
private[finagle] class InterpreterFactory[Req, Rep](
    uninterpreted: UninterpretedName,
    newFactory: Var[Addr] => ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver = NullStatsReceiver,
    maxCacheSize: Int = 8) 
  extends ServiceFactory[Req, Rep] {
  require(maxCacheSize > 0)

  private[this] val UninterpretedName(getNamer, tree) = uninterpreted
  @volatile private[this] var cache =
    Map.empty: Map[Namer, IdlingFactory[Req, Rep]]

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

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val namer = getNamer()

    readLock.lock()
    try {
      if (cache contains namer)
        return cache(namer).apply(conn)
    } finally {
      readLock.unlock()
    }

    miss(namer, conn)
  }
  
  private[this] def miss(namer: Namer, conn: ClientConnection)
  : Future[Service[Req, Rep]] = {
    writeLock.lock()

    // Make sure it's still a miss.
    if (cache contains namer) {
      writeLock.unlock()
      return cache(namer).apply(conn)
    }

    val watch = Stopwatch.start()

    val svc = try {
      nmiss.incr()

      val factory = new IdlingFactory(
        newFactory(namer.bindAndEval(tree)))

      if (cache.size < maxCacheSize) {
        cache += (namer -> factory)
        cache(namer).apply(conn)
      } else {
        findEvictee() match {
          case Some(evicted) => 
            nevict.incr()
            cache(evicted).close()
            cache = cache - evicted + (namer -> factory)
            cache(namer).apply(conn)
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
      Trace.record("Interpreter cache miss with namer "+namer+": "+d, d)
      misstime.add(d.inMilliseconds)
    }
  }

  def oneshot(factory: ServiceFactory[Req, Rep], conn: ClientConnection)
  : Future[Service[Req, Rep]] =
    factory(conn) map { service =>
      new ServiceProxy(service) {
        override def close(deadline: Time) =
          super.close(deadline) transform { case _ =>
            factory.close(deadline)
          }
      }
    }

  def findEvictee(): Option[Namer] = {
    val (evictNamer, evictFactory) = cache maxBy { case (_, fac) => fac.idleFor }
    if (evictFactory.idleFor > Duration.Zero) Some(evictNamer)
    else None
  }

  def close(deadline: Time) =
    Closable.all(cache.values.toSeq:_*).close(deadline)

  override def isAvailable =
    cache.values.exists(_.isAvailable)
}
