package com.twitter.finagle.util

import java.util.ArrayDeque
import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.twitter.util.{Time, TimerTask, Duration}

/**
 * A key-less LIFO cache that supports TTLs.
 *
 * Why LIFO? In the presence of a TTL, we want to reuse the most recently
 * used items in the cache, so that we need fewer of them.
 *
 * @param cacheSize the maximum size that the cache will not exceed
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

  // We assume monotonically increasing time.  Thus the items at the
  // end of the deque are also the newest (i.e. LIFO behavior).
  private[this] var deque = new ArrayDeque[(Time, A)]
  private[this] var timerTask: Option[TimerTask] = None

  /**
   * Removes expired items from deque, starting from "last" (oldest)
   * @returns expired items
   *
   * Assumes that internal order of return value (seq of expired
   * items) does not matter
   *
   * This call does *not* evict the expired items.
   *
   * Implementation: Assume that there are relatively few items to
   * evict, so it is cheaper to traverse from old->new than new->old
   */
  private[this] def removeExpiredItems(): Seq[A] = synchronized {
    val deadline = Time.now - ttl

    @tailrec
    def constructExpiredList(acc: List[A]): List[A] = {
      Option(deque.peekLast) match {
        case Some((ts, item)) if ts <= deadline =>
          // should ditch *oldest* items, so take from deque's last
          deque.removeLast()
          constructExpiredList(item :: acc)
        case _ =>
          // assumes time monotonicity (all items below the split
          //   point are old, all items above the split point are
          //   young)
          acc
      }
    }
    constructExpiredList(Nil)
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
      val es = removeExpiredItems()
      if (!deque.isEmpty) scheduleTimer()
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
    if (!deque.isEmpty) {
      val rv = Some(deque.pop()._2)
      if (deque.isEmpty) cancelTimer()
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
      if (deque.isEmpty && ttl != Duration.Top) scheduleTimer()
      deque.push((Time.now, item))
      if (deque.size > cacheSize) { // it will ever only be over by 1
        // should ditch *oldest* items, so take from last of deque
        val (time, oldest) = deque.removeLast()
        Some(oldest)
      } else {
        None
      }
    }
    evicted foreach { evict(_) }
  }

  /**
   * Evict all items, clearing the cache.
   */
  def evictAll() = {
    val evicted = synchronized {
      val oldDeque = deque
      deque = new ArrayDeque[(Time, A)]  // clear deque
      cancelTimer()
      oldDeque
    }

    evicted.asScala foreach { case (_, item) => evict(item) }
  }

  /**
   * The current size of the cache.
   */
  def size = synchronized { deque.size }
}
