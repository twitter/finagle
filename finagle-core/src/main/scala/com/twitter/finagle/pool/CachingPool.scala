package com.twitter.finagle.pool

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.Cache
import com.twitter.finagle.{
  ClientConnection, Service, ServiceClosedException, ServiceFactory, ServiceProxy, WriteException
}
import com.twitter.util.{Future, Time, Duration, Timer, Promise, Throw}
import scala.annotation.tailrec

/**
 * A pool that temporarily caches items from the underlying one, up to
 * the given timeout amount of time.
 */
private[finagle] class CachingPool[Req, Rep](
  factory: ServiceFactory[Req, Rep],
  cacheSize: Int,
  ttl: Duration,
  timer: Timer,
  statsReceiver: StatsReceiver = NullStatsReceiver)
  extends ServiceFactory[Req, Rep]
{
  private[this] val cache =
    new Cache[Service[Req, Rep]](cacheSize, ttl, timer, Some(_.close()))
  @volatile private[this] var isOpen = true
  private[this] val sizeGauge =
    statsReceiver.addGauge("pool_cached") { cache.size }

  private[this] class WrappedService(underlying: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](underlying)
  {
    override def close(deadline: Time) =
      if (this.isAvailable && CachingPool.this.isOpen) {
        cache.put(underlying)
        Future.Done
      } else
        underlying.close(deadline)
  }

  @tailrec
  private[this] def get(): Option[Service[Req, Rep]] = {
    cache.get() match {
      case s@Some(service) if service.isAvailable => s
      case Some(service) /* unavailable */ => service.close(); get()
      case None => None
    }
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = synchronized {
    if (!isOpen) Future.exception(new ServiceClosedException) else {
      get() match {
        case Some(service) =>
          Future.value(new WrappedService(service))
        case None =>
          newConn(conn)
      }
    }
  }

  private[this] def newConn(conn: ClientConnection) = {
    val p = new Promise[Service[Req, Rep]]

    val underlying = factory(conn) map { new WrappedService(_) }
    underlying respond { p.updateIfEmpty(_) }

    p.setInterruptHandler { case e =>
      if (p.updateIfEmpty(Throw(WriteException(e))))
        underlying onSuccess { _.close() }
    }

    p
  }

  def close(deadline: Time) = synchronized {
    isOpen = false

    cache.evictAll()
    factory.close(deadline)
  }

  override def isAvailable = isOpen && factory.isAvailable

  override val toString = "caching_pool_%s".format(factory.toString)
}
