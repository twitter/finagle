package com.twitter.finagle.pool

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{
  ClientConnection,
  Service,
  ServiceClosedException,
  ServiceFactory,
  ServiceProxy,
  Status
}
import com.twitter.util.{Duration, Future, NullTimerTask, Time, Timer}
import java.util.ArrayList
import java.util.concurrent.LinkedBlockingDeque
import java.util.function.Predicate
import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 * A pool that temporarily caches items from the underlying one, up to
 * the given timeout amount of time.
 *
 * @param ttl time-to-live for Services not in use. Note: Collection is
 *            run at most per TTL, thus the "real" TTL is a uniform distribution
 *            in the range [ttl, ttl * 2)
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#caching-pool user guide]]
 *      for more details.
 */
private[finagle] final class CachingPool[Req, Rep](
  factory: ServiceFactory[Req, Rep],
  cacheSize: Int,
  ttl: Duration,
  timer: Timer,
  statsReceiver: StatsReceiver = NullStatsReceiver)
    extends ServiceFactory[Req, Rep] { self =>

  require(cacheSize > 0)

  private[this] val pool = new Pool()

  private[this] val wrap: Service[Req, Rep] => Service[Req, Rep] = svc =>
    new WrappedService(svc, ttl.fromNow)

  @volatile private[this] var _isOpen = true

  private[this] val sizeGauge =
    statsReceiver.addGauge("pool_cached") {
      pool.size
    }

  @tailrec
  private[this] def poolGet(): WrappedService = {
    // note this loop is expected to terminate. if the pool is empty,
    // we bail and will create a new instance. if the pool returns
    // a closed service, it has now been removed, and we'll try again
    // until the pool is empty or returns a non-closed one.
    val service = pool.checkout()
    if (service == null) null
    else if (service.status != Status.Closed) service
    else {
      service.underlying.close()
      poolGet()
    }
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (!isOpen) Future.exception(new ServiceClosedException)
    else {
      val service = poolGet()
      if (service == null) factory(conn).map(wrap)
      else Future.value(service)
    }
  }

  def close(deadline: Time): Future[Unit] = {
    _isOpen = false

    sizeGauge.remove()
    pool.drainAndClose()

    factory.close(deadline)
  }

  private def isOpen: Boolean = _isOpen

  override def status: Status =
    if (isOpen) factory.status
    else Status.Closed

  override def toString: String = s"caching_pool_$factory"

  private final class Pool {

    private[this] val items = new LinkedBlockingDeque[WrappedService](self.cacheSize)

    private[this] val timerTask =
      if (!self.ttl.isFinite) {
        NullTimerTask
      } else {
        self.timer.schedule(self.ttl) {
          items.removeIf(new Predicate[WrappedService] {
            def test(wrapped: WrappedService): Boolean = {
              val shouldExpire = Time.now >= wrapped.expiry
              if (shouldExpire) wrapped.underlying.close()
              shouldExpire
            }
          })
        }
      }

    /**
     * Returns `null` if there are no services in the pool.
     */
    def checkout(): WrappedService = {
      items.pollFirst()
    }

    def checkin(service: Service[Req, Rep]): Unit = {
      val added = items.offerFirst(new WrappedService(service, self.ttl.fromNow))
      if (!added) {
        // the queue was full and this element could not be put back into
        // the pool. close it as nobody else will be able to access it.
        service.close()
      }
    }

    def size: Int = items.size

    /**
     * This can be called multiple times.
     *
     * Each call will drain all the WrappedServices from the pool and close them all.
     */
    def drainAndClose(): Unit = {
      timerTask.cancel()
      val copy = new ArrayList[WrappedService]()
      items.drainTo(copy)
      copy.asScala.foreach(_.underlying.close())
    }
  }

  /**
   * These are what get returned to the callers of [[CachingPool.apply]].
   */
  private final class WrappedService(val underlying: Service[Req, Rep], val expiry: Time)
      extends ServiceProxy[Req, Rep](underlying) {

    // access mediated by synchronizing on `this`. note that contention on it
    // should be close to non-existent as it would be rare for multiple threads
    // to call `close` concurrently.
    private[this] var closeF: Future[Unit] = null

    // We ensure that a service wrapper instance is only ever closed once to defend against
    // inserting the same underlying service into the cache multiple times.
    override def close(deadline: Time): Future[Unit] = synchronized {
      if (closeF eq null) {
        if (status != Status.Closed) {
          pool.checkin(underlying)
          // we need to workaround the race of the CachingPool getting closed (and drained)
          // before this gets put back into the pool.
          // that would strand those services. so, after checking it back in, check if
          // its no longer open, and if not, drain again.
          if (!CachingPool.this.isOpen) {
            pool.drainAndClose()
          }
          closeF = Future.Done
        } else {
          closeF = underlying.close(deadline)
        }
      }
      closeF
    }
  }

}
