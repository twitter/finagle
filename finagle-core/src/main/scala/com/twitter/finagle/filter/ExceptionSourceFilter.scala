package com.twitter.finagle.filter

import com.twitter.finagle.{SimpleFilter, Service, SourcedException}
import com.twitter.util.Future

class ExceptionSourceFilter[Req, Rep](serviceName: String) extends SimpleFilter[Req, Rep] {
  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    service(req) onFailure {
      case e: SourcedException => e.serviceName = serviceName
      case _ => ()
    }
}
