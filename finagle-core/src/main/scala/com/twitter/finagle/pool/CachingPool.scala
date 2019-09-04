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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Predicate
import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 * A pool that temporarily caches items from the underlying one, up to
 * the given timeout amount of time.
 *
 * @param ttl time-to-live for Services not in use. Note: Collection is
 *            run at most per TTL, thus the "real" TTL is a uniform distribution
 *            in the range [ttl, ttl * 2). Put this way, it is acceptable
 *            to lag the session expiration for at most TTL or 1 second, whichever is shorter.
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

  import CachingPool._

  require(cacheSize > 0)

  private[this] val pool = new Pool[Req, Rep](cacheSize, ttl, timer)

  private[this] val sizeGauge =
    statsReceiver.addGauge("pool_cached") {
      pool.size
    }

  /**
   * These are what get returned to the callers of [[CachingPool.apply]].
   *
   * These are "single use" and as such not intended to be reused across checkouts
   * from the `Pool`.
   */
  private[this] final class WrappedService(underlying: Service[Req, Rep])
      extends ServiceProxy[Req, Rep](underlying) {

    // access mediated by synchronizing on `this`. note that contention on it
    // should be close to non-existent as it would be rare for multiple threads
    // to call `close` concurrently.
    private[this] var closed = false

    // We ensure that a service wrapper instance is only ever closed once to defend against
    // inserting the same underlying service into the cache multiple times.
    override def close(deadline: Time): Future[Unit] = {
      val closeUnderlying = synchronized {
        if (closed) {
          false // already back in the pool, nothing to do.
        } else {
          closed = true
          !pool.checkin(underlying)
        }
      }
      // wasn't added back to the pool, close the underlying service.
      if (closeUnderlying)
        underlying.close(deadline)
      Future.Done
    }
  }

  private[this] def wrap(service: Service[Req, Rep]): WrappedService =
    new WrappedService(service)

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (!isOpen) Future.exception(new ServiceClosedException)
    else {
      val service = pool.checkout()
      if (service == null) factory(conn).map(wrap)
      else Future.value(wrap(service))
    }
  }

  def close(deadline: Time): Future[Unit] = {
    sizeGauge.remove()
    pool.drainAndClose()

    factory.close(deadline)
  }

  private[this] def isOpen: Boolean = pool.isOpen

  override def status: Status =
    if (isOpen) factory.status
    else Status.Closed

  override def toString: String = s"caching_pool_$factory"

}

/**
 * Exposed for testing purposes.
 */
private[pool] object CachingPool {

  /**
   * A concurrent/thread-safe last in/first out (LIFO) pool of `Services`.
   * It exposes an optional idle TTL for how long they can sit in the pool.
   */
  final class Pool[Req, Rep](maxSize: Int, ttl: Duration, timer: Timer) {

    /**
     * Used to hold a [[Service]] in the [[Pool]].
     *
     * It's `AtomicBoolean` state represents whether or not this item has been
     * checked out from the pool. This is used because while `LinkedBlockingDeque`
     * is a concurrent data structure, it's iterators (including `removeIf`) may
     * return stale values. [[maybeCheckout]] is used to mediate access,
     * and callers must get `true` in order to use an instance of [[Idle]].
     */
    private[this] final class Idle(val service: Service[Req, Rep], val expiry: Time)
        extends AtomicBoolean(false) {

      /**
       * Attempt to acquire exclusive access to the service.
       *
       * Note: the expectation is that contention will be low. Typically this
       * is called by `Pool.checkout` and only occasionally by `Pool.timerTask`.
       *
       * @return true if acquired, false otherwise.
       */
      def maybeCheckout(): Boolean =
        compareAndSet(false, true)
    }

    private[this] val items = new LinkedBlockingDeque[Idle](maxSize)

    @volatile private[this] var _isOpen = true

    def isOpen: Boolean = _isOpen

    private[this] val timerTask =
      if (!ttl.isFinite) {
        NullTimerTask
      } else {
        timer.schedule(ttl.min(Duration.fromSeconds(1))) {
          items.removeIf(new Predicate[Idle] {
            def test(idle: Idle): Boolean = {
              if (Time.now < idle.expiry) {
                false
              } else {
                if (idle.maybeCheckout()) {
                  idle.service.close()
                  true
                } else {
                  false
                }
              }
            }
          })
        }
      }

    /**
     * Returns `null` if there are no Services in the pool.
     *
     * If a non-null result is returned, the caller is given
     * exclusive access until it is returned to the pool.
     */
    @tailrec
    def checkout(): Service[Req, Rep] = {
      val idle = items.pollFirst()
      if (idle == null) {
        null // nothing left in the pool
      } else if (idle.maybeCheckout()) {
        if (idle.service.status == Status.Closed) {
          // got one that was already closed. try again. note that while
          // a `Service` may signal that it is unusable with `Status.Closed`,
          // it does not guarantee that it's ever had `Service.close` called.
          // this does so in order to assure that resources are cleaned up.
          idle.service.close()
          checkout()
        } else {
          idle.service
        }
      } else {
        checkout() // got one that raced with someone else getting it. try again.
      }
    }

    /**
     * Return the underlying `Service` back to the pool.
     *
     * The underlying `Service` will be made available for [[checkout]],
     * if it is still valid and there is room in the pool.
     *
     * @return true if the underlying `Service` is made available,
     *         false otherwise.
     */
    def checkin(service: Service[Req, Rep]): Boolean = {
      if (service.status != Status.Closed && items.offerFirst(new Idle(service, ttl.fromNow))) {
        // we need to workaround the race of the CachingPool getting closed (and drained)
        // before the WrappedService gets put back into the pool.
        // that would strand those services. so, after checking it back in, check if
        // its no longer open, and if not, drain again.
        if (!isOpen) {
          drainAndClose()
        }
        true
      } else {
        false
      }
    }

    def size: Int = items.size

    /**
     * This can be called multiple times.
     *
     * Each call will drain all Services from the pool and close them all.
     */
    def drainAndClose(): Unit = {
      _isOpen = false
      timerTask.cancel()
      val copy = new ArrayList[Idle](size)
      items.drainTo(copy)
      copy.asScala.foreach(_.service.close())
    }
  }

}
