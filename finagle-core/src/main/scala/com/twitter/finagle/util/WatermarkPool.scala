package com.twitter.finagle.util

import annotation.tailrec
import collection.mutable.Queue

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.{Pool, Future, Promise, Return}

/**
 * A pool facotry manages object lifecycle in a pool. Specifically:
 * creation, destruction & health checking.
 */
trait PoolFactory[A] {
  /**
   * Create a new item. This call cannot block. Instead return an
   * (asynchronous) Future.
   */
  def make(): Future[A]

  /**
   * The given item has been end-of-life'd.
   */
  def dispose(item: A): Unit

  /**
   * Query the health of the given item.
   */
  def isHealthy(item: A): Boolean
}

/**
 * The watermark pool is an object pool with low & high
 * watermarks. This behaves as follows: the pool will persist up to
 * the low watermark number of items (as long as they have been
 * created), and won't start queueing requests until the high
 * watermark has been reached. Put another way: up to `lowWatermark'
 * items may persist indefinitely, while there are at no times more
 * than `highWatermark' items in concurrent existence.
 */
class WatermarkPool[A](
    factory: PoolFactory[A],
    lowWatermark: Int, highWatermark: Int = Int.MaxValue)
  extends Pool[A]
{
  private[this] val queue = Queue[A]()
  private[this] val waiters = Queue[Promise[A]]()
  private[this] var numItems = 0

  private[this] def make(): Future[A] = {
    numItems += 1
    factory.make()
  }

  private[this] def dispose(item: A) {
    factory.dispose(item)
    numItems -= 1
  }

  @tailrec private[this] def dequeue(): Option[A] = {
    if (queue.isEmpty) {
      None
    } else {
      val item = queue.dequeue()
      if (!factory.isHealthy(item))
        dequeue()
      else
        Some(item)
    }
  }

  def reserve(): Future[A] = synchronized {
    dequeue() match {
      case Some(item) =>
        Future.value(item)
      case None if numItems < highWatermark =>
        make()
      case None =>
        val promise = new Promise[A]
        waiters += promise
        promise
    }
  }

  def release(item: A) = synchronized {
    if (!factory.isHealthy(item)) {
      dispose(item)
      // If we just disposed of an item, and this bumped us beneath
      // the high watermark, then we are free to satisfy the first
      // waiter.
      if (numItems < highWatermark && !waiters.isEmpty) {
        val waiter = waiters.dequeue()
        make() respond { value => waiter() = value }
      }
    } else if (!waiters.isEmpty) {
      val waiter = waiters.dequeue()
      waiter() = Return(item)
    } else if (numItems <= lowWatermark) {
      queue += item
    } else {
      dispose(item)
    }
  }
}
