package com.twitter.finagle.pool

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.Cache
import com.twitter.finagle.{
  ClientConnection,
  Service,
  ServiceClosedException,
  ServiceFactory,
  ServiceProxy,
  Status
}
import com.twitter.util.{Duration, Future, Time, Timer}
import scala.annotation.tailrec

/**
 * A pool that temporarily caches items from the underlying one, up to
 * the given timeout amount of time.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#caching-pool user guide]]
 *      for more details.
 */
private[finagle] class CachingPool[Req, Rep](
  factory: ServiceFactory[Req, Rep],
  cacheSize: Int,
  ttl: Duration,
  timer: Timer,
  statsReceiver: StatsReceiver = NullStatsReceiver
) extends ServiceFactory[Req, Rep] {
  private[this] val cache =
    new Cache[Service[Req, Rep]](cacheSize, ttl, timer, Some(_.close()))
  @volatile private[this] var isOpen = true
  private[this] val sizeGauge =
    statsReceiver.addGauge("pool_cached") { cache.size }


  final private[this] class WrappedService(underlying: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](underlying) {

    // access mediated by synchronizing on CachingPool.this
    private[this] var close: Future[Unit] = null

    // We ensure that a service wrapper instance is only ever closed once to defend against
    // inserting the same underlying service into the cache multiple times.
    override def close(deadline: Time): Future[Unit] =
      CachingPool.this.synchronized {
        if (close eq null) {
          if (this.status != Status.Closed && CachingPool.this.isOpen) {
            cache.put(underlying)
            close = Future.Done
          } else {
            close = underlying.close(deadline)
          }
        }
        close
      }
  }

  @tailrec
  private[this] def get(): Option[Service[Req, Rep]] = cache.get() match {
    case s @ Some(service) if service.status != Status.Closed => s
    case Some(service) /* unavailable */ => service.close(); get()
    case None => None
  }

  private[this] val wrap: Service[Req, Rep] => Service[Req, Rep] = svc => new WrappedService(svc)

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = CachingPool.this.synchronized {
    if (!isOpen) Future.exception(new ServiceClosedException)
    else {
      get() match {
        case Some(service) =>
          Future.value(new WrappedService(service))
        case None =>
          factory(conn).map(wrap)
      }
    }
  }

  def close(deadline: Time) = CachingPool.this.synchronized {
    isOpen = false

    cache.evictAll()
    factory.close(deadline)
  }

  override def status =
    if (isOpen) factory.status
    else Status.Closed

  override val toString = "caching_pool_%s".format(factory.toString)
}
