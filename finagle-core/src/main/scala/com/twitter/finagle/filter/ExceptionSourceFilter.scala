package com.twitter.finagle.filter

import com.twitter.finagle.{SimpleFilter, Service, SourcedException}
import com.twitter.util.Future

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
