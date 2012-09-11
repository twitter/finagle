package com.twitter.finagle.pool

import com.twitter.util.{Future, Promise, Return, Throw}
import com.twitter.finagle.{
  Service, ServiceFactory, ServiceClosedException,
  TooManyWaitersException, ServiceProxy,
  CancelledConnectionException, ClientConnection}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import scala.annotation.tailrec
import scala.collection.mutable.Queue

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
{
  private[this] val queue       = Queue[ServiceWrapper]()
  private[this] val waiters     = Queue[Promise[Service[Req, Rep]]]()
  private[this] var numServices = 0
  private[this] var isOpen      = true

  private[this] val waitersStat = statsReceiver.addGauge("pool_waiters") { synchronized { waiters.size } }
  private[this] val sizeStat = statsReceiver.addGauge("pool_size") { synchronized { numServices } }

  /**
   * Flush waiters by creating new services for them. This must
   * be called whenever we decrease the service count.
   */
  private[this] def flushWaiters() = synchronized {
    while (numServices < highWatermark && !waiters.isEmpty) {
      val waiter = waiters.dequeue()
      val res = this() respond { waiter() = _ }
      waiter.linkTo(res)
    }
  }

  private[this] class ServiceWrapper(underlying: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](underlying)
  {
    override def release() = WatermarkPool.this.synchronized {
      if (!isOpen) {
        underlying.release()
        numServices -= 1
      } else if (!isAvailable) {
        underlying.release()
        numServices -= 1
        // If we just disposed of an service, and this bumped us beneath
        // the high watermark, then we are free to satisfy the first
        // waiter.
        flushWaiters()
      } else if (!waiters.isEmpty) {
        val waiter = waiters.dequeue()
        waiter() = Return(this)
      } else if (numServices <= lowWatermark) {
        queue += this
      } else {
        underlying.release()
        numServices -= 1
      }
    }
  }

  @tailrec private[this] def dequeue(): Option[Service[Req, Rep]] = {
    if (queue.isEmpty) {
      None
    } else {
      val service = queue.dequeue()
      if (!service.isAvailable) {
        // Note: since these are ServiceWrappers, accounting is taken
        // care of by ServiceWrapper.release()
        service.release()
        dequeue()
      } else {
        Some(service)
      }
    }
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = synchronized {
    if (!isOpen)
      return Future.exception(new ServiceClosedException)

    dequeue() match {
      case Some(service) =>
        Future.value(service)
      case None if numServices < highWatermark =>
        numServices += 1
        factory(conn) onFailure { _ =>
          synchronized {
            numServices -= 1
            flushWaiters()
          }
        } map { new ServiceWrapper(_) }
      case None if waiters.size >= maxWaiters =>
        Future.exception(new TooManyWaitersException)
      case None =>
        val promise = new Promise[Service[Req, Rep]]
        waiters += promise
        promise onCancellation {
          // remove ourselves from the waitlist if we're still there.
          val dq = synchronized { waiters.dequeueFirst { _ eq promise } }
          dq foreach { _() = Throw(new CancelledConnectionException) }
        }
        promise
    }
  }

  def close() = synchronized {
    // Mark the pool closed, relinquishing completed requests &
    // denying the issuance of further requests. The order here is
    // important: we mark the service unavailable before releasing the
    // individual channels so that they are actually released in the
    // wrapper.
    isOpen = false

    // Drain the pool.
    queue foreach { _.release() }
    queue.clear()

    // Kill the existing waiters.
    waiters foreach { _() = Throw(new ServiceClosedException) }
    waiters.clear()

    // Close the underlying factory.
    factory.close()
  }

  override def isAvailable = isOpen && factory.isAvailable

  override val toString = "watermark_pool_%s".format(factory.toString)
}
