package com.twitter.finagle.filter

import com.twitter.finagle.{
  param,
  SimpleFilter,
  Service,
  ServiceFactory,
  SourcedException,
  Stack,
  Stackable,
  Failure
}
import com.twitter.util.Future

object ExceptionSourceFilter {
  val role: Stack.Role = Stack.Role("ExceptionSource")

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.ExceptionSourceFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Label, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = ExceptionSourceFilter.role
      val description: String = "Source exceptions to the service name"
      def make(_label: param.Label, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val param.Label(label) = _label
        new ExceptionSourceFilter(label) andThen next
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that sources exceptions. The `serviceName`
 * field of any [[com.twitter.finagle.SourcedException]] thrown by the
 * underlying [[com.twitter.finagle.Service]] is set to the `serviceName`
 * argument of this filter.
 */
class ExceptionSourceFilter[Req, Rep](serviceName: String) extends SimpleFilter[Req, Rep] {

  private[this] val addExceptionSource: PartialFunction[Throwable, Future[Rep]] = {
    case f: Failure =>
      Future.exception(f.withSource(Failure.Source.Service, serviceName))
    case e: SourcedException =>
      e.serviceName = serviceName
      Future.exception(e)
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    service(request).rescue(addExceptionSource)
}
