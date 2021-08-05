package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.util._
import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * A factory that won't satisfy the service future until an underlying service factory
 * is ready.
 *
 * `close` closes the underlying service factory, which means that it won't be
 * satisfied until after the underlying future has been satisfied.
 *
 * @note Implicitly masks the underlying future from interrupts.
 * Promises are detached on interruption.
 *
 * @param underlyingF The future should be satisfied when the underlying factory is ready
 */
class DelayedFactory[Req, Rep](underlyingF: Future[ServiceFactory[Req, Rep]])
    extends ServiceFactory[Req, Rep] {
  private[this] def wrapped(): Future[ServiceFactory[Req, Rep]] = safelyInterruptible(underlyingF)

  private[this] val q = new ConcurrentLinkedQueue[Promise[ServiceFactory[Req, Rep]]]()

  underlyingF ensure {
    q.clear()
  }

  private[this] def safelyInterruptible(
    f: Future[ServiceFactory[Req, Rep]]
  ): Future[ServiceFactory[Req, Rep]] = {
    val p = Promise.attached(f)
    p setInterruptHandler {
      case t: Throwable =>
        if (p.detach()) {
          q.remove(p)
          p.setException(Failure.adapt(t, FailureFlags.Interrupted))
        }
    }
    q.add(p)
    p
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    wrapped flatMap { fac => fac(conn) }

  override def close(deadline: Time): Future[Unit] = {
    if (underlyingF.isDefined) wrapped flatMap { svc => svc.close(deadline) }
    else {
      underlyingF.onSuccess(_.close(deadline))
      // It may be unsafe to pass as retryable so unset retryable flag
      val exc = (new ServiceClosedException).unflagged(FailureFlags.Retryable)
      underlyingF.raise(exc)
      for (p <- q.asScala)
        p.raise(exc)
      Future.Done
    }
  }

  override def status: Status =
    if (underlyingF.isDefined) Await.result(underlyingF).status
    else Status.Busy

  private[finagle] def numWaiters(): Int = q.size()

  override def toString: String = s"DelayedFactory(waiters=${numWaiters()})"
}

object DelayedFactory {

  /**
   * Returns a [[com.twitter.finagle.ServiceFactory]] backed by a [[DelayedFactory]] until the
   * underlying completes.  Upon completion, it swaps and is just backed by the underlying.
   */
  def swapOnComplete[Req, Rep](
    underlying: Future[ServiceFactory[Req, Rep]]
  ): ServiceFactory[Req, Rep] = {
    val delayed = new DelayedFactory(underlying)

    val ref = new ServiceFactoryRef[Req, Rep](delayed)
    underlying respond {
      case Throw(e) => ref() = new FailingFactory(e)
      case Return(fac) => ref() = fac
    }
    ref
  }

}
