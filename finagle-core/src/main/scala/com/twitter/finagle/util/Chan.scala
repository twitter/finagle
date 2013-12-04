package com.twitter.finagle.util

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.{Logger, Level}

trait Chan[-T] {
  def !(elem: T)
  def close()
}

/**
 * A Proc is a process that can receive messages. Procs guarantee
 * that exactly one message is delivered at a time, and that they are
 * delivered in the order received.
 *
 * They can be thought of as featherweight actors.
 */
trait Proc[-T] extends Chan[T] {
  private[this] val q = new ConcurrentLinkedQueue[T]
  private[this] val nq = new AtomicInteger(0)
  @volatile private[this] var closed = false

  def close() { closed = true }

  def !(elem: T) {
    q.offer(elem)
    if (nq.getAndIncrement() == 0)
      do {
        val elem = q.poll()
        // Swallow exceptions as these would cause
        // unbounded queue growth.
        if (!closed) {
          try receiver(elem) catch {
             case exc: Throwable =>
               Logger.getLogger("").log(Level.WARNING, "Exception thrown in proc", exc)
           }
        }
      } while (nq.decrementAndGet() > 0)
  }

  def receive: T => Unit
  private[this] val receiver = receive
}

object Proc {
  def apply[T](iteratee: T => Unit): Proc[T] = new Proc[T] {
    def receive = iteratee
  }

  val nil: Proc[Any] = new Proc[Any] { def receive = Function.const(()) }
}
