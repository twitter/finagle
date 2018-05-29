package com.twitter.finagle

import com.twitter.util.{Closable, Future, Time}
import scala.util.control.NonFatal

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
