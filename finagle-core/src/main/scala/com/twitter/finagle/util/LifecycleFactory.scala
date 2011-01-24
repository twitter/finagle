package com.twitter.finagle.util

import collection.mutable.Queue

import com.twitter.util.{Future, Time, Duration}

/**
 * Manage object lifecycle [eg. in a pool]. Specifically: creation,
 * destruction & health checking.
 */
trait LifecycleFactory[A] {
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
 * A LifecycleFactory wrapper that temporarily caches items from the
 * underlying one, up to the given timeout amount of time. Note that
 * cache cleanup is clocked at the same duration, and so the
 * timekeeping is sloppy: an item may indeed be persisted for any time
 * in the range [timeout, 2 x timeout].
 */
class CachingLifecycleFactory[A](
    underlying: LifecycleFactory[A],
    timeout: Duration,
    timer: com.twitter.util.Timer = Timer.default)
  extends LifecycleFactory[A]
{
  private[this] val deathRow = Queue[(Time, A)]()

  private[this] def collect(): Unit = synchronized {
    val now = Time.now
    val dequeued = deathRow dequeueAll { case (timestamp, _) => timestamp.until(now) > timeout }
    dequeued foreach { case (_, service) => underlying.dispose(service) }
    if (!deathRow.isEmpty)
      timer.schedule(timeout.fromNow)(collect)
  }

  def make(): Future[A] = synchronized {
    while (!deathRow.isEmpty) {
      val (_, item) = deathRow.dequeue()
      if (isHealthy(item))
        return Future.value(item)
    }

    underlying.make()
  }

  def dispose(item: A) = synchronized {
    if (isHealthy(item)) {
      if (deathRow.isEmpty)
        timer.schedule(timeout.fromNow)(collect)
      deathRow += ((Time.now, item))
    } else {
      underlying.dispose(item)
    }
  }

  def isHealthy(item: A) = underlying.isHealthy(item)
}
