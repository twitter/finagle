package com.twitter.finagle.filter

import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack, Stackable}
import com.twitter.util.{Future, Promise}

private[finagle] object MaskCancelFilter {
  val role = Stack.Role("MaskCancel")

  // TODO: we should simply transform the stack for boolean
  // stackables like this.
  case class Param(yesOrNo: Boolean)
  implicit object Param extends Stack.Param[Param] {
    val default = Param(false)
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.MaskCancelFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[Param, ServiceFactory[Req, Rep]] {
      val role = MaskCancelFilter.role
      val description = "Prevent cancellations from propagating to other services"
      def make(_param: Param, next: ServiceFactory[Req, Rep]) = {
        _param match {
          case Param(true) => new MaskCancelFilter[Req, Rep] andThen next
          case _ => next
        }
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that prevents cancellations from propagating
 * to any subsequent [[com.twitter.finagle.Service Services]]. i.e. when
 * `Future.raise` is invoked on the result of this filter's `apply` method, the
 * interrupt will not be propagated to the service. This is useful for
 * lightweight protocols for which finishing a request is preferable to closing
 * and reesstablishing a connection.
 */
class MaskCancelFilter[Req, Rep] extends SimpleFilter[Req, Rep] {
  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    service(req).masked
}
