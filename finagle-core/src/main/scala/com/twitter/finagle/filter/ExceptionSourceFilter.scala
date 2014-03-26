package com.twitter.finagle.filter

import com.twitter.finagle.{param, SimpleFilter, Service,
  ServiceFactory, SourcedException, Stack, Stackable}
import com.twitter.util.Future

private[finagle] object ExceptionSourceFilter {
  object ExceptionSource extends Stack.Role

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.ExceptionSourceFilter]].
   */
   def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
     new Stack.Simple[ServiceFactory[Req, Rep]](ExceptionSource) {
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val param.Label(label) = params[param.Label]
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
  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    service(req) onFailure {
      case e: SourcedException => e.serviceName = serviceName
      case _ => ()
    }
}
