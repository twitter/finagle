package com.twitter.finagle

import com.twitter.util.{Closable, Future, Promise, Return, Time, Throw, Try}
import scala.util.control.{NonFatal, NoStackTrace}

object Service {

  /**
   * Wrap the given service such that any synchronously thrown `NonFatal`
   * exceptions are lifted into `Future.exceptions`.
   */
  def rescue[Req, Rep](service: Service[Req, Rep]): ServiceProxy[Req, Rep] =
    new ServiceProxy[Req, Rep](service) {
      override def apply(request: Req): Future[Rep] = {
        try {
          service(request)
        } catch {
          case NonFatal(e) => Future.exception(e)
        }
      }
      override def toString: String = service.toString
    }

  /**
   * A convenience method for creating `Services` from a `Function1` of
   * `Req` to a `Future[Rep]`.
   */
  def mk[Req, Rep](f: Req => Future[Rep]): Service[Req, Rep] = new Service[Req, Rep] {
    def apply(req: Req): Future[Rep] = f(req)
  }

  /**
   * A service with a constant reply. Always available; never closable.
   *
   * @see [[constant]] for a Java compatible API.
   */
  def const[Rep](rep: Future[Rep]): Service[Any, Rep] =
    new service.ConstantService(rep)

  /** Java compatible API for [[const]] as `const` is a reserved word in Java */
  def constant[Rep](rep: Future[Rep]): Service[Any, Rep] =
    Service.const(rep)

  /**
   * A future of a service is a pending service.
   *
   * Requests wait behind resolution of the future. If resolution fails, all
   * requests to this service reflect the same failure. While resolution is
   * pending, the status of the service is Busy, otherwise it's whatever the
   * underlying service reports.
   *
   * Closing the service is uninterruptible and instant: failing all subsequent
   * requests, and interrupting those in flight. Close returns
   * [[com.twitter.util.Future.Done Future.Done]] always, independent of the
   * underlying service.
   *
   * {{{
   * val svc = Service.pending(Future.never)
   * val rep = svc(1)
   * val closed = svc.close()
   *
   * Await.result(closed) // Future.Done
   * Await.result(rep)    // Exception: Service is closed
   * }}}
   *
   * Note: Because queued requests rely on queue order of future callbacks,
   * it's not guaranteed that requests to the underlying service will be
   * received in the same order they were sent.
   */
  def pending[Req, Rep](svc: Future[Service[Req, Rep]]): Service[Req, Rep] =
    new Pending(svc)

  private class Pending[Req, Rep](fs: Future[Service[Req, Rep]]) extends Service[Req, Rep] {
    import Pending._

    @volatile private[this] var state: Pending.State[Service[Req, Rep]] = NotReady
    private[this] val closep: Promise[Service[Req, Rep]] = new Promise[Service[Req, Rep]]
    private[this] def absorbInterrupt: Future[Service[Req, Rep]] = fs.interruptible()

    fs.respond { t => state = Ready(t) }

    def apply(req: Req): Future[Rep] =
      state match {
        case Closed => Future.exception(ClosedException)
        case NotReady => closep.or(absorbInterrupt).flatMap(svc => svc(req))
        case Ready(Return(svc)) => svc(req)
        case Ready(t @ Throw(_)) => Future.const(t.cast[Rep])
      }

    override def status: Status =
      state match {
        case Closed => Status.Closed
        case NotReady => Status.Busy
        case Ready(Throw(exc)) => Status.Closed
        case Ready(Return(svc)) => svc.status
      }

    override def close(deadline: Time): Future[Unit] = {
      state = Closed
      if (closep.updateIfEmpty(Throw(ClosedException))) {
        fs.flatMap(svc => svc.close(deadline))
      }
      Future.Done
    }
  }

  private object Pending {
    object ClosedException extends Exception("Service is closed") with NoStackTrace

    sealed trait State[+A]
    case object Closed extends State[Nothing]
    case object NotReady extends State[Nothing]
    case class Ready[A](a: Try[A]) extends State[A]
  }
}

/**
 * A `Service` is an asynchronous function from a `Request` to a `Future[Response]`.
 *
 * It is the basic unit of an RPC interface.
 *
 * @see The [[https://twitter.github.io/finagle/guide/ServicesAndFilters.html#services user guide]]
 *      for details and examples.
 *
 * @see [[com.twitter.finagle.Service.mk Service.mk]] for a convenient
 *     way to create new instances.
 */
abstract class Service[-Req, +Rep] extends (Req => Future[Rep]) with Closable {
  def map[Req1](f: Req1 => Req): Service[Req1, Rep] = new Service[Req1, Rep] {
    def apply(req1: Req1): Future[Rep] = Service.this.apply(f(req1))
    override def close(deadline: Time): Future[Unit] = Service.this.close(deadline)
  }

  /**
   * This is the method to override/implement to create your own Service.
   */
  def apply(request: Req): Future[Rep]

  def close(deadline: Time): Future[Unit] = Future.Done

  /**
   * The current availability [[Status]] of this `Service`.
   */
  def status: Status = Status.Open

  /**
   * Determines whether this `Service` is available (can accept requests
   * with a reasonable likelihood of success).
   */
  final def isAvailable: Boolean = status == Status.Open

  override def toString: String = getClass.getName
}
