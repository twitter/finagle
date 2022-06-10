package com.twitter.finagle.filter

import com.twitter.finagle.param
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SourcedException
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.Failure
import com.twitter.finagle.server.ServerInfo
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
        val appId = ServerInfo().id
        new ExceptionSourceFilter(label, appId) andThen next
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that sources exceptions. The `serviceName` and `appId`
 * fields of any [[com.twitter.finagle.SourcedException]] thrown by the
 * underlying [[com.twitter.finagle.Service]] is set to the `serviceName` and `appId`
 * arguments of this filter.
 */
class ExceptionSourceFilter[Req, Rep](serviceName: String, appId: String)
    extends SimpleFilter[Req, Rep] {

  private[this] val addExceptionSource: PartialFunction[Throwable, Future[Rep]] = {
    case f: Failure =>
      Future.exception(
        f.withSource(Failure.Source.Service, serviceName).withSource(Failure.Source.AppId, appId))
    case e: SourcedException =>
      e.serviceName = serviceName
      e.appId = appId
      Future.exception(e)
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    service(request).rescue(addExceptionSource)
}
