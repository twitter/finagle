package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Future, Promise, Time, Throw, Return}
import java.util.ArrayDeque
import scala.annotation.tailrec
import scala.collection.JavaConverters._

object WatermarkPool {
  private val TooManyWaiters = Future.exception(new TooManyWaitersException)
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
 */
class WatermarkPool[Req, Rep](
    factory: ServiceFactory[Req, Rep],
    lowWatermark: Int, highWatermark: Int = Int.MaxValue,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    maxWaiters: Int = Int.MaxValue)
  extends ServiceFactory[Req, Rep]
{ thePool => // note: avoids `self` as an alias because ServiceProxy has a `self`

  private[this] val queue       = new ArrayDeque[ServiceWrapper]()
  private[this] val waiters     = new ArrayDeque[Promise[Service[Req, Rep]]]()
  private[this] var numServices = 0
  @volatile private[this] var isOpen      = true

  private[this] val numWaiters = statsReceiver.counter("pool_num_waited")
  private[this] val tooManyWaiters = statsReceiver.counter("pool_num_too_many_waiters")
  private[this] val waitersStat = statsReceiver.addGauge("pool_waiters") {
    thePool.synchronized { waiters.size }
  }
  private[this] val sizeStat = statsReceiver.addGauge("pool_size") {
    thePool.synchronized { numServices }
  }

  /**
   * Flush waiters by creating new services for them. This must
   * be called whenever we decrease the service count.
   */
  private[this] def flushWaiters() = thePool.synchronized {
    while (numServices < highWatermark && !waiters.isEmpty) {
      val waiter = waiters.removeFirst()
      waiter.become(this())
    }
  }

  private[this] class ServiceWrapper(underlying: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](underlying)
  {
    override def close(deadline: Time) = {
      val releasable = thePool.synchronized {
        if (!isOpen) {
          numServices -= 1
          true
        } else if (status == Status.Closed) {
          numServices -= 1
          // If we just disposed of an service, and this bumped us beneath
          // the high watermark, then we are free to satisfy the first
          // waiter.
          flushWaiters()
          true
        } else if (!waiters.isEmpty) {
          val waiter = waiters.removeFirst()
          waiter() = Return(this)
          false
        } else if (numServices <= lowWatermark) {
          queue.addLast(this)
          false
        } else {
          numServices -= 1
          true
        }
      }

      if (releasable)
        underlying.close(deadline)
      else
        Future.Done
    }
  }

  @tailrec private[this] def dequeue(): Option[Service[Req, Rep]] = {
    if (queue.isEmpty) {
      None
    } else {
      val service = queue.removeFirst()
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

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (!isOpen)
      return Future.exception(new ServiceClosedException)
    thePool.synchronized {
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
          p.setInterruptHandler { case _cause =>
            if (thePool.synchronized(waiters.remove(p))) {
              val failure = Failure.adapt(
                new CancelledConnectionException(_cause),
                Failure.Restartable|Failure.Interrupted)
              p.setException(failure)
            }
          }
          return p
      }
    }

    // If we reach this point, we've committed to creating a service
    // (numServices was increased by one).
    val p = new Promise[Service[Req, Rep]]
    val underlying = factory(conn).map { new ServiceWrapper(_) }
    underlying.respond { res =>
      p.updateIfEmpty(res)
      if (res.isThrow) thePool.synchronized {
        numServices -= 1
        flushWaiters()
      }
    }
    p.setInterruptHandler { case e =>
      val failure = Failure.adapt(e, Failure.Restartable|Failure.Interrupted)
      if (p.updateIfEmpty(Throw(failure)))
        underlying.onSuccess { _.close() }
    }
    p
  }

  def close(deadline: Time): Future[Unit] = thePool.synchronized {
    // Mark the pool closed, relinquishing completed requests &
    // denying the issuance of further requests. The order here is
    // important: we mark the service unavailable before releasing the
    // individual channels so that they are actually released in the
    // wrapper.
    isOpen = false

    // Drain the pool.
    queue.asScala foreach { _.close() }
    queue.clear()

    // Kill the existing waiters.
    waiters.asScala foreach { _() = Throw(new ServiceClosedException) }
    waiters.clear()

    // Close the underlying factory.
    factory.close(deadline)
  }

  override def status: Status =
    if (isOpen) factory.status
    else Status.Closed

  override val toString: String = "watermark_pool_%s".format(factory.toString)
}
