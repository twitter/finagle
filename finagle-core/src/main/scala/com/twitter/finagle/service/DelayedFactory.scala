package com.twitter.finagle.service

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory}
import com.twitter.util.{Await, Closable, Future, Promise, Time, Throw, Return}

/**
 * A factory that won't satisfy the service future until an underlying service factory
 * is ready.
 *
 * Close closes the underlying service factory, which means that it won't be
 * satisfied until after the underlying future has been satisfied.
 *
 * @note Implicitly masks the underlying future from interrupts.
 * Promises are detached on interruption.
 *
 * @param underlyingF The future should be satisfied when the underlying factory is ready
 */
class DelayedFactory[Req, Rep](
  underlyingF: Future[ServiceFactory[Req, Rep]]
) extends ServiceFactory[Req, Rep] {
  private[this] def wrapped: Future[ServiceFactory[Req, Rep]] =
    safelyInterruptible(wrapped)

  private[this] def safelyInterruptible(
    f: Future[ServiceFactory[Req, Rep]]): Future[ServiceFactory[Req, Rep]] = {
    val p = Promise.attached(f)
    p setInterruptHandler { case t: Throwable =>
        if (p.detach())
          p.setException(t)
    }
    p
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = wrapped.flatMap(_(conn))

  override def close(deadline: Time) = wrapped.flatMap(_.close(deadline))
  override def isAvailable = underlyingF.isDefined && Await.result(underlyingF).isAvailable
}

object DelayedFactory {
  def swapOnComplete[Req, Rep](f: Future[ServiceFactory[Req, Rep]], c: Closable): ServiceFactory[Req, Rep] = {
    val delayed = new DelayedFactory(f) {
      override def close(deadline: Time): Future[Unit] =
        Future.join(Seq(c.close(deadline), super.close(deadline)))
    }

    val ref = new ServiceFactoryRef[Req, Rep](delayed)
    f respond {
      case Throw(e) => ref() = new FailingFactory(e)
      case Return(fac) => ref() = fac
    }
    ref
  }

}
