package com.twitter.finagle.server

import com.twitter.util.{Closable, CloseAwaitably, Future, Time}
import java.lang.{Boolean => JBoolean}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.JavaConverters._

/**
 * Manage a collection of `Closable`s
 *
 * Upon closing, all registered `Closable`s are closed and elements registered in the future
 * are immediately closed with the deadline provided by the first call to `close`.
 */
private final class Closables extends Closable with CloseAwaitably {
  private[this] val registeredClosables =
    Collections.newSetFromMap(new ConcurrentHashMap[Closable, JBoolean])
  private[this] var closeDeadline: Option[Time] = None
  private[this] val lock = new ReentrantReadWriteLock()

  // we want to avoid registering a new closable after we've initiated closing
  // but it's fine for registration to race, so we lock registration with
  // readLock and closing with writeLock.
  private[this] val readLock = lock.readLock()
  private[this] val writeLock = lock.writeLock()

  /**
   * Register the `Closable`.
   *
   * If `close` has not been called, the `Closable` is added to the set of instances which
   * will be closed when the `close` method is called on this `Closables`.
   *
   * @see `remove` to unregister a `Closable`.
   */
  def register(closable: Closable): Unit = {
    // We need to determine if we have already been closed, and if not, register the
    // `Closable`. If we have been closed already, we pass the deadline out of the
    // synchronized block so we don't invoke the arbitrary `close` method while holding
    // the lock.
    readLock.lock()
    val deadline =
      try {
        if (closeDeadline.isEmpty) {
          registeredClosables.add(closable)
        }
        closeDeadline
      } finally {
        readLock.unlock()
      }

    deadline match {
      case Some(deadline) => closable.close(deadline)
      case None => // nop: close hasn't been called yet
    }
  }

  /**
   * Unregister a `Closable`.
   *
   * Removes the `Closable` from the set of instances which will be closed when `this.close`
   * is called.
   *
   * @see `register` to register a `Closable`.
   *
   * @return True if the closable was registered, False otherwise.
   */
  def unregister(closable: Closable): Boolean = {
    readLock.lock()
    try {
      registeredClosables.remove(closable)
    } finally {
      readLock.unlock()
    }
  }

  def close(deadline: Time): Future[Unit] = closeAwaitably {
    writeLock.lock()
    val toClose =
      try {
        closeDeadline = Some(deadline)
        val allClosables = registeredClosables.asScala.toVector
        registeredClosables.clear()
        allClosables
      } finally {
        writeLock.unlock()
      }

    Closable.all(toClose: _*).close(deadline)
  }
}
