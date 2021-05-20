package com.twitter.finagle.client

import com.twitter.finagle.{Name, Service, ServiceClosedException, Stack, Status}
import com.twitter.util.{Closable, Future, Promise, Time}

/**
 * A method pool that materializes at max one client for all outstanding methods (or endpoints)
 * built with [[com.twitter.finagle.client.MethodBuilder]].
 *
 * It's thread-safe so multiple threads can call into materialize/get/open/close concurrently.
 *
 * It's reusable so after it's fully closed it can be materialized and opened again. This
 * functionality allows for a single `MethodBuilder` to be shared between multiple methods (or
 * endpoints), each of whose can have an independent lifecycle. Put another way, closing all methods
 * produced with a given `MethodBuilder` SHOULD NOT affect its ability to create more methods in the
 * future.
 *
 * The proper usage for this class is to call `materialize` before anything else.
 */
private[finagle] final class MethodPool[Req, Rep](
  stackClient: StackBasedClient[Req, Rep],
  dest: Name,
  label: String)
    extends Closable {

  sealed abstract class State(val service: Service[Req, Rep])

  case object Closed
      extends State(new Service[Req, Rep] {
        def apply(request: Req): Future[Rep] = Future.exception(new ServiceClosedException)
        override def status: Status = Status.Closed
      })

  case class Opened(s: Service[Req, Rep], count: Int, onClose: Promise[Unit]) extends State(s)

  private[this] var state: State = Closed

  def get: Service[Req, Rep] = synchronized(state.service)

  /**
   * Materializes the client without affecting the ref counting. If the client is already
   * materialized, this method is a no-op.
   */
  def materialize(params: Stack.Params): Unit = synchronized {
    state match {
      case Closed =>
        state = Opened(stackClient.withParams(params).newService(dest, label), 0, new Promise[Unit])
      case Opened(_, _, _) =>
        ()
    }
  }

  /**
   * Opens this method pool.
   */
  def open(): Unit = synchronized {
    state match {
      case Closed =>
        throw new IllegalStateException("Can't open. Need to materialize first")
      case Opened(s, count, onClose) =>
        state = Opened(s, count + 1, onClose)
    }
  }

  /**
   * Closes this method pool, which may close the actual (materialized) client.
   */
  def close(deadline: Time): Future[Unit] = synchronized {
    state match {
      case Closed => Future.Done
      case Opened(s, n, onClose) =>
        if (n == 0) {
          throw new IllegalStateException("Can't close what was never opened.")
        } else if (n == 1) {
          state = Closed
          onClose.become(s.close(deadline))
        } else {
          state = Opened(s, n - 1, onClose)
        }

        onClose
    }
  }
}
