package com.twitter.finagle.pool

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.Cache
import com.twitter.finagle.{
  ClientConnection, Service, ServiceClosedException, ServiceFactory, ServiceProxy, 
  Status}
import com.twitter.util.{Future, Time, Duration, Timer}
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
      if (this.status != Status.Closed && CachingPool.this.isOpen) {
        cache.put(underlying)
        Future.Done
      } else
        underlying.close(deadline)
  }

  @tailrec
  private[this] def get(): Option[Service[Req, Rep]] = {
    cache.get() match {
      case s@Some(service) if service.status != Status.Closed => s
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
          factory(conn) map { new WrappedService(_) }
      }
    }
  }

  def close(deadline: Time) = synchronized {
    isOpen = false

    cache.evictAll()
    factory.close(deadline)
  }

  override def status = 
    if (isOpen) factory.status
    else Status.Closed

  override val toString = "caching_pool_%s".format(factory.toString)
}
