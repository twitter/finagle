package com.twitter.finagle.util

import com.twitter.util.{Duration, Time, TimerTask}
import java.util.{ArrayDeque, ArrayList}
import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 * A key-less LIFO cache that supports TTLs.
 *
 * Why LIFO? In the presence of a TTL, we want to reuse the most recently
 * used items in the cache, so that we need fewer of them.
 *
 * @note This class is thread-safe.
 *
 * @param cacheSize the maximum size that the cache will not exceed
 * @param ttl time-to-live for cached objects.  Note: Collection is
 * run at most per TTL, thus the "real" TTL is a uniform distribution
 * in the range [ttl, ttl * 2)
 * @param timer the timer used to schedule TTL evictions
 * @param evictor a Function invoked for each eviction
 */
private[finagle] class Cache[A](
  cacheSize: Int,
  ttl: Duration,
  timer: com.twitter.util.Timer,
  evictor: Option[A => Unit] = None: None.type) {
  require(cacheSize > 0)

  // Thread-safety for state is provided by synchronization on `this`.

  // We assume monotonically increasing time.  Thus the items at the
  // end of the deque are also the newest (i.e. LIFO behavior).
  //
  // The two ArrayDeques work together to hold each item it's expiry.
  // So an item in the front of one will have it's expiry at the
  // front of the other.
  //
  // A reasonable starting size is used, balancing the max size (cacheSize)
  // and not too eagerly allocating the initial underlying array.
  private[this] val expirations = new ArrayDeque[Time](math.min(cacheSize + 1, 128))
  private[this] val items = new ArrayDeque[A](math.min(cacheSize + 1, 128))
  private[this] var timerTask: TimerTask = null

  /**
   * Removes expired items from deque, starting from "last" (oldest).
   *
   * Assumes that internal order of return value (seq of expired
   * items) does not matter
   *
   * This call does *not* evict the expired items.
   *
   * Implementation: Assume that there are relatively few items to
   * evict, so it is cheaper to traverse from old->new than new->old
   *
   * @return expired items
   */
  private[this] def removeExpiredItems(): Seq[A] = synchronized {
    val deadline = Time.now - ttl

    @tailrec
    def constructExpiredList(acc: List[A]): List[A] = {
      val ts = expirations.peekLast()
      if (ts != null && ts <= deadline) {
        // should ditch *oldest* items, so take from the end
        expirations.removeLast()
        val item = items.removeLast()
        constructExpiredList(item :: acc)
      } else {
        // assumes time monotonicity (all items below the split
        //   point are old, all items above the split point are
        //   young)
        acc
      }
    }
    constructExpiredList(Nil)
  }

  private[this] def scheduleTimer(): Unit = synchronized {
    require(timerTask == null)
    timerTask = timer.schedule(ttl.fromNow) { timeout() }
  }

  private[this] def cancelTimer(): Unit = synchronized {
    if (timerTask != null) {
      timerTask.cancel()
      timerTask = null
    }
  }

  private[this] def timeout(): Unit = {
    val evicted = synchronized {
      timerTask = null
      val es = removeExpiredItems()
      if (!expirations.isEmpty) scheduleTimer()
      es
    }
    evicted.foreach(evict)
  }

  private[this] def evict(item: A): Unit = {
    evictor match {
      case Some(e) => e(item)
      case None => ()
    }
  }

  /**
   * Retrieve an item from the cache.  Items are retrieved in LIFO
   * order.
   */
  def get(): Option[A] = synchronized {
    if (expirations.isEmpty) {
      None
    } else {
      expirations.removeFirst()
      val rv = Some(items.removeFirst())
      if (expirations.isEmpty) cancelTimer()
      rv
    }
  }

  /**
   * Insert an item into the cache.
   */
  def put(item: A): Unit = {
    val evicted = synchronized {
      if (expirations.isEmpty && ttl != Duration.Top) scheduleTimer()
      expirations.addFirst(Time.now)
      items.addFirst(item)
      if (expirations.size > cacheSize) { // it will ever only be over by 1
        // should ditch *oldest* items, so take from last
        expirations.removeLast()
        items.removeLast()
      } else {
        null
      }
    }
    if (evicted != null) {
      evict(evicted.asInstanceOf[A])
    }
  }

  /**
   * Evict all items, clearing the cache.
   */
  def evictAll(): Unit = {
    val evicted = synchronized {
      val oldItems = new ArrayList[A](items)
      items.clear()
      expirations.clear()
      cancelTimer()
      oldItems
    }

    evicted.asScala.foreach(evict)
  }

  /**
   * The current size of the cache.
   */
  def size: Int = synchronized { expirations.size }
}
