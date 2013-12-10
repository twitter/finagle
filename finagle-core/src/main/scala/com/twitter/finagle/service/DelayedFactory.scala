package com.twitter.finagle.service

import com.twitter.finagle.{CancelledConnectionException, ClientConnection, ServiceClosedException,
  Service, ServiceFactory, ServiceNotAvailableException}
import com.twitter.util.{Await, Future, Promise, Return, Throw, Time, Try}
import scala.collection.JavaConverters._
import java.util.ArrayDeque

/**
 * A factory that won't satisfy the service future until an underlying service factory
 * is ready.
 *
 * Close closes the underlying service factory, which means that it won't be
 * satisfied until after the underlying future has been satisfied.
 *
 * @note Implicitly masks the underlying future from interrupts.  Manually manages promises
 * so that they will be detached on interruption.
 *
 * @param underlyingF The future should be satisfied when the underlying factory is ready
 */
class DelayedFactory[Req, Rep](
  underlyingF: Future[ServiceFactory[Req, Rep]]
) extends ServiceFactory[Req, Rep] {

  sealed trait State
  case class AwaitingFactory(
    q: ArrayDeque[(ClientConnection, Promise[Service[Req, Rep]])]
  ) extends State
  case class AwaitingRelease(p: Promise[Unit], deadline: Time, cause: Throwable) extends State
  case class Failed(exc: Throwable) extends State
  case class Succeeded(f: ServiceFactory[Req, Rep]) extends State

  @volatile private[this] var state: State = AwaitingFactory(new ArrayDeque())

  underlyingF respond {
    case Return(factory) => synchronized {
      state = state match {
        case Succeeded(_) | Failed(_) =>
          throw new IllegalStateException("it should be impossible to get in this state: " +
            state +
            " after a successful future satisfaction")
        case AwaitingRelease(p, deadline, cause) =>
          p.become(factory.close(deadline))
          Failed(cause)
        case AwaitingFactory(q) =>
          for ((conn, p) <- q.asScala)
            p.become(factory(conn))
          Succeeded(factory)
      }
    }
    case Throw(exc) => synchronized {
      state = state match {
        case Succeeded(_) | Failed(_) =>
          throw new IllegalStateException("it should be impossible to get in this state: " +
            state +
            " after a failed future satisfaction")
        case AwaitingRelease(p, _, _) =>
          p.setValue(())
          Failed(new CancelledConnectionException(exc))
        case AwaitingFactory(q) =>
          q.asScala foreach { case (_, p) =>
            p.setException(exc)
          }
          Failed(exc)
      }
    }
  }

  // if your future is never satisfied, or takes a long time to satisfy, you can accumulate
  // many closures.  if you interrupt your future, it will detach the closure.
  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = state match {
    case AwaitingRelease(_, _, cause) => Future.exception(cause)
    case Failed(exc) => Future.exception(exc)
    case Succeeded(factory) => factory(conn)
    case state => applySlow(conn)
  }

  private[this] def applySlow(conn: ClientConnection): Future[Service[Req, Rep]] = synchronized {
    state match {
      case AwaitingFactory(q) =>
        val p = Promise[Service[Req, Rep]]
        val waiter = (conn, p)
        q.addLast(waiter)
        p.setInterruptHandler { case cause: Throwable =>
          state match {
            case AwaitingFactory(q) =>
              if (synchronized(q.remove(waiter)))
                p.setException(new CancelledConnectionException(cause))
            case Succeeded(_) | Failed(_) | AwaitingRelease(_, _, _) =>
          }
        }
        p
      case AwaitingRelease(_, _, cause) => Future.exception(cause)
      case Failed(exc) => Future.exception(exc)
      case Succeeded(factory) => factory(conn)
    }
  }

  override def close(deadline: Time) = synchronized {
    state match {
      case Succeeded(factory) => factory.close(deadline)
      case Failed(exc) => Future.exception(exc)
      case AwaitingRelease(p, old, exc) =>
        state = AwaitingRelease(p, old min deadline, exc)
        p
      case AwaitingFactory(q) =>
        val exc = new ServiceClosedException
        val onRelease = Promise[Unit]()
        underlyingF.raise(exc)
        for ((_, p) <- q.asScala)
          p.raise(exc)
        state = AwaitingRelease(onRelease, deadline, exc)
        onRelease
    }
  }

  private[service] def numWaiters(): Int = synchronized {
    state match {
      case AwaitingFactory(q) => q.size()
      case _ => -1
    }
  }

  override def isAvailable = underlyingF.isDefined && Await.result(underlyingF).isAvailable
}
