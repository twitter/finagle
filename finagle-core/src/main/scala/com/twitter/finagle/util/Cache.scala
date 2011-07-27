package com.twitter.finagle.util

import scala.collection.mutable.{Queue, ArrayBuffer}
import scala.annotation.tailrec

import com.twitter.util.{Time, TimerTask, Duration}

/**
 * A key-less cache that supports TTLs.
 *
 * @param cacheSize the maximum size which the cache will not exceed
 * @param ttl time-to-live for cached objects.  Note: Collection is
 * run at most per TTL, thus the "real" TTL is a uniform distribution
 * in the range [ttl, ttl * 2)
 * @param timer the timer used to schedule TTL evictions
 * @param evictor a Function invoked for each eviction
 */
private[finagle] class Cache[A](
  cacheSize: Int, ttl: Duration,
  timer: com.twitter.util.Timer,
  evictor: Option[A => Unit] = None)
{
  require(cacheSize > 0)

  // We assume monotonically increasing time.  thus the items at the
  // head of the queue are also the oldest.
  private[this] val queue = new Queue[(Time, A)]
  private[this] var timerTask: Option[TimerTask] = None

  private[this] def dequeueExpiredItems(): Seq[A] = synchronized {
    val now = Time.now

    @tailrec
    def loop(acc: List[A]): List[A] = {
      queue.headOption match {
        case Some((ts, item)) if ts.until(now) >= ttl =>
          queue.dequeue()
          loop(item :: acc)
        case _ =>
          // we can safely terminate here because of time monoticity.
          acc
      }
    }

    loop(Nil)
  }

  private[this] def scheduleTimer(): Unit = synchronized {
    require(!timerTask.isDefined)
    timerTask = Some(timer.schedule(ttl.fromNow) { timeout() })
  }

  private[this] def cancelTimer() = synchronized {
    timerTask foreach { _.cancel() }
    timerTask = None
  }

  private[this] def timeout() = {
    val evicted = synchronized {
      timerTask = None
      val es = dequeueExpiredItems()
      if (!queue.isEmpty) scheduleTimer()
      es
    }
    evicted foreach { evict(_) }
  }

  private[this] def evict(item: A) = evictor foreach { _(item) }

  /**
   * Retrieve an item from the cache.  Items are retrieved in LIFO
   * order.
   */
  def get() = synchronized {
    if (!queue.isEmpty) {
      val rv = Some(queue.dequeue()._2)
      if (queue.isEmpty) cancelTimer()
      rv
    } else {
      None
    }
  }

  /**
   * Insert an item into the cache.
   */
  def put(item: A) {
    val evicted = synchronized {
      if (queue.isEmpty) scheduleTimer()
      queue += ((Time.now, item))
      if (queue.size > cacheSize)  // it will ever only be over by 1
        Some(queue.dequeue()._2)
      else
        None
    }

    evicted foreach { evict(_) }
  }

  /**
   * Evict all items, clearing the cache.
   */
  def evictAll() = {
    val evicted = synchronized {
      val buf = queue.toBuffer
      queue.clear()
      cancelTimer()
      buf
    }

    evicted foreach { case (_, item) => evict(item) }
  }

  /**
   * The current size of the cache.
   */
  def size = synchronized { queue.size }
}
