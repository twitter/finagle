package com.twitter.finagle.client

import com.twitter.util.{Closable, Future, Promise, Time}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * Delays closing the underlying [[Closable]] until the
 * reference count reaches 0.
 *
 * @param closable the resource that is reference counted.
 *
 * @note this does not handle calling [[close]] before [[open]]
 *       or users calling [[close]] directly on the underlying
 *       resource.
 */
private[finagle] class RefcountedClosable[T <: Closable](closable: T) extends Closable {

  private[this] val count = new AtomicInteger(0)
  private[this] val isClosed = new AtomicBoolean(false)
  private[this] val closedP = new Promise[Unit]()

  def get: T =
    closable

  /** Increase the reference count */
  def open(): Unit =
    count.incrementAndGet()

  /**
   * Decrease the reference count.
   *
   * If, and only if, this is the first time the reference count
   * drops down to 0, the `close` is called on the underlying
   * [[Closable]].
   *
   * @return a [[Future]] that is satisfied only when the underlying
   *         [[Closable.close()]] is satisfied.
   */
  override def close(deadline: Time): Future[Unit] = {
    // when we reach 0 outstanding refs and this is the first
    // time to do that, close the underlying service
    if (count.decrementAndGet() == 0 && isClosed.compareAndSet(false, true)) {
      closedP.become(closable.close(deadline))
    }
    closedP
  }

}
