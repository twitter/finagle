package com.twitter.finagle.pool

import annotation.tailrec
import collection.mutable.Queue

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.{Future, Promise, Return, Throw}

import com.twitter.finagle.{Service, ServiceFactory, ServiceClosedException}
import com.twitter.finagle.util.AsyncLatch

/**
 * The watermark pool is an object pool with low & high
 * watermarks. This behaves as follows: the pool will persist up to
 * the low watermark number of items (as long as they have been
 * created), and won't start queueing requests until the high
 * watermark has been reached. Put another way: up to `lowWatermark'
 * items may persist indefinitely, while there are at no times more
 * than `highWatermark' items in concurrent existence.
 */
class WatermarkPool[Req, Rep](
    factory: ServiceFactory[Req, Rep],
    lowWatermark: Int, highWatermark: Int = Int.MaxValue)
  extends ServiceFactory[Req, Rep]
{
  private[this] val queue       = Queue[Service[Req, Rep]]()
  private[this] val waiters     = Queue[Promise[Service[Req, Rep]]]()
  private[this] var numServices = 0
  private[this] var isOpen      = true

  private[this] class ServiceWrapper(underlying: Service[Req, Rep])
    extends Service[Req, Rep]
  {
    def apply(request: Req) = underlying(request)

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
        if (numServices < highWatermark && !waiters.isEmpty) {
          val waiter = waiters.dequeue()
          make() respond { waiter() = _ }
        }
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

    override def isAvailable = underlying.isAvailable
  }

  @tailrec private[this] def dequeue(): Option[Service[Req, Rep]] = {
    if (queue.isEmpty) {
      None
    } else {
      val service = queue.dequeue()
      if (!service.isAvailable) {
        service.release()
        dequeue()
      } else {
        Some(service)
      }
    }
  }

  def make(): Future[Service[Req, Rep]] = synchronized {
    if (!isOpen)
      return Future.exception(new ServiceClosedException)

    dequeue() match {
      case Some(service) =>
        Future.value(service)
      case None if numServices < highWatermark =>
        numServices += 1
        factory.make() map { new ServiceWrapper(_) }
      case None =>
        val promise = new Promise[Service[Req, Rep]]
        waiters += promise
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

  override def isAvailable = isOpen
}
