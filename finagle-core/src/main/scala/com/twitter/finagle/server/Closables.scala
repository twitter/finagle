package com.twitter.finagle.server

import com.twitter.util.{Closable, CloseAwaitably, Future, Time}
import scala.collection.JavaConverters._

/**
 * Manage a collection of `Closable`s
 *
 * Upon closing, all registered `Closable`s are closed and elements registered in the future
 * are immediately closed with the deadline provided by the first call to `close`.
 */
private final class Closables extends Closable with CloseAwaitably {
  // We use the `registeredClosables` collection as our intrinsic lock
  private[this] val registeredClosables = new java.util.HashSet[Closable]
  private[this] var closeDeadline: Option[Time] = None

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
    val deadline = registeredClosables.synchronized {
      if (closeDeadline.isEmpty) {
        registeredClosables.add(closable)
      }
      closeDeadline
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
  def unregister(closable: Closable): Boolean = registeredClosables.synchronized {
    registeredClosables.remove(closable)
  }

  def close(deadline: Time): Future[Unit] = closeAwaitably {
    val toClose = registeredClosables.synchronized {
      closeDeadline = Some(deadline)
      val allClosables = registeredClosables.asScala.toVector
      registeredClosables.clear()
      allClosables
    }

    Closable.all(toClose:_*).close(deadline)
  }
}
