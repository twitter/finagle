package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Future, Promise, Return, Throw, Time, Try}
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.collection.JavaConverters._

private object WatermarkPool {
  private val TooManyWaiters = Future.exception(new TooManyWaitersException)

  private val CloseOnSuccess: Try[Service[_, _]] => Unit = {
    case Return(svc) => svc.close()
    case _ => ()
  }
}

/**
 * The watermark pool is an object pool with low & high
 * watermarks. It keeps the number of services from a given service
 * factory in a certain range.
 *
 * This behaves as follows: the pool will persist up to
 * the low watermark number of items (as long as they have been
 * created), and won't start queueing requests until the high
 * watermark has been reached. Put another way: up to `lowWatermark'
 * items may persist indefinitely, while there are at no times more
 * than `highWatermark' items in concurrent existence.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#watermark-pool user guide]]
 *      for more details.
 */
final class WatermarkPool[Req, Rep](
  factory: ServiceFactory[Req, Rep],
  lowWatermark: Int,
  highWatermark: Int = Int.MaxValue,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  maxWaiters: Int = Int.MaxValue)
    extends ServiceFactory[Req, Rep] {
  thePool => // note: avoids `self` as an alias because ServiceProxy has a `self`

  // `queue` contains unwrapped `Service` instances, which *must* be wrapped by a `ServiceWrapper` before
  // returning to the application.

  // Note on thread-safety: `queue`, `waiters`, `numServices` and `isOpen` are mutable state and
  // are accessed while holding the `thePool` as a lock. This is categorically true for accessing
  // `waiters` and `numServices`.
  //
  // There are two exceptions when we don't acquire the lock:
  //
  // 1) When reading `isOpen` we permit a race in status as status is inherently racey.
  // 2) We skip lock acquisition when accessing `queue` after checking `isOpen`. See the
  //    longer explanation in #close.

  private[this] val queue = new ArrayDeque[Service[Req, Rep]]()
  private[this] val waiters = new ArrayDeque[Promise[Service[Req, Rep]]]()
  private[this] var numServices = 0
  @volatile private[this] var isOpen = true

  private[this] val numWaiters = statsReceiver.counter("pool_num_waited")
  private[this] val tooManyWaiters = statsReceiver.counter("pool_num_too_many_waiters")
  private[this] val waitersGauge = statsReceiver.addGauge("pool_waiters") {
    thePool.synchronized { waiters.size }
  }
  private[this] val sizeGauge = statsReceiver.addGauge("pool_size") { size }

  /**
   * The current size of the pool.
   *
   * Exposed for testing.
   */
  private[pool] def size: Int = synchronized(numServices)

  /**
   * Flush waiters by creating new services for them. This must
   * be called whenever we decrease the service count.
   */
  @tailrec
  private[this] def flushWaiters(): Unit = {
    var waiter: Promise[Service[Req, Rep]] = null
    thePool.synchronized {
      if (numServices < highWatermark && !waiters.isEmpty) {
        waiter = waiters.removeFirst()
      }
    }

    if (waiter == null) ()
    else {
      waiter.become(this())
      flushWaiters()
    }
  }

  final private[this] class ServiceWrapper(underlying: Service[Req, Rep])
      extends ServiceProxy[Req, Rep](underlying) {

    private[this] val closed: Promise[Unit] = new Promise[Unit]
    private[this] val released: AtomicBoolean = new AtomicBoolean(false)

    override def close(deadline: Time): Future[Unit] = {
      // ensure that we update the accounting only once per wrapper instance
      if (released.compareAndSet(false, true)) {
        var waiter: Promise[Service[Req, Rep]] = null
        var needsFlush: Boolean = false
        val releasable = thePool.synchronized {
          if (!isOpen) {
            numServices -= 1
            true
          } else if (status == Status.Closed) {
            numServices -= 1
            // If we just disposed of an service, and this bumped us beneath
            // the high watermark, then we are free to satisfy the first
            // waiter.
            needsFlush = true
            true
          } else if (!waiters.isEmpty) {
            waiter = waiters.removeFirst()
            false
          } else if (numServices <= lowWatermark) {
            queue.addLast(underlying)
            false
          } else {
            numServices -= 1
            true
          }
        }

        if (needsFlush) flushWaiters()
        if (waiter != null) waiter.setValue(new ServiceWrapper(this.underlying))

        if (releasable)
          closed.become(underlying.close(deadline))
        else
          closed.setDone()
      }

      closed
    }
  }

  @tailrec private[this] def dequeue(): Option[ServiceWrapper] = {
    if (queue.isEmpty) {
      None
    } else {
      val service = new ServiceWrapper(queue.removeFirst())
      if (service.status == Status.Closed) {
        // Note: since these are ServiceWrappers, accounting is taken
        // care of by ServiceWrapper.close()
        service.close()
        dequeue()
      } else {
        Some(service)
      }
    }
  }

  private[this] val wrap: Service[Req, Rep] => Service[Req, Rep] = svc => new ServiceWrapper(svc)

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    thePool.synchronized {
      if (!isOpen)
        return Future.exception(new ServiceClosedException)
      dequeue() match {
        case Some(service) =>
          return Future.value(service)
        case None if numServices < highWatermark =>
          numServices += 1
        case None if waiters.size >= maxWaiters =>
          tooManyWaiters.incr()
          return WatermarkPool.TooManyWaiters
        case None =>
          val p = new Promise[Service[Req, Rep]]
          numWaiters.incr()
          waiters.addLast(p)
          p.setInterruptHandler {
            case _cause =>
              if (thePool.synchronized(waiters.remove(p))) {
                val failure = Failure.adapt(
                  new CancelledConnectionException(_cause),
                  FailureFlags.Retryable | FailureFlags.Interrupted
                )
                p.setException(failure)
              }
          }
          return p
      }
    }

    // If we reach this point, we've committed to creating a service
    // (numServices was increased by one).
    val p = new Promise[Service[Req, Rep]]
    val underlying = factory(conn).map(wrap)
    underlying.respond { res =>
      p.updateIfEmpty(res)
      var needsFlush: Boolean = false
      if (res.isThrow) thePool.synchronized {
        numServices -= 1
        needsFlush = true
      }
      if (needsFlush) flushWaiters()
    }
    p.setInterruptHandler {
      case e =>
        val failure = Failure.adapt(e, FailureFlags.Retryable | FailureFlags.Interrupted)
        if (p.updateIfEmpty(Throw(failure)))
          underlying.respond(WatermarkPool.CloseOnSuccess)
    }
    p
  }

  def close(deadline: Time): Future[Unit] = {
    // Mark the pool closed, relinquishing completed requests &
    // denying the issuance of further requests. The order here is
    // important: we mark the service unavailable before releasing the
    // individual channels so that they are actually released in the
    // wrapper.
    val toFail = thePool.synchronized {
      isOpen = false

      // nb: we can't lean on the `isOpen` bit flip protecting us as we
      // can with the `queue` drain which follows because of the interrupt
      // handler above which accesses `waiters`. Also, note the significant
      // call to `toArray` which copies waiters to a new collection instead
      // of proxying to the underlying one.
      val res = waiters.toArray(new Array[Promise[Service[Req, Rep]]](waiters.size))
      waiters.clear()
      res
    }

    // Fail the existing waiters.
    toFail.foreach { waiter => waiter.setException(new ServiceClosedException) }

    // Drain the pool. All `queue` access first tests `isOpen` mediated by `thePool` lock
    // so we don't need to hold the lock while clearing it since we've flipped the `isOpen`
    // bit.
    queue.asScala.foreach { svc => (new ServiceWrapper(svc)).close() }
    queue.clear()

    // Clear out the gauges
    sizeGauge.remove()
    waitersGauge.remove()

    // Close the underlying factory.
    factory.close(deadline)
  }

  override def status: Status =
    if (isOpen) factory.status
    else Status.Closed

  override val toString: String = "watermark_pool_%s".format(factory.toString)
}
