package com.twitter.finagle.exp.routing

import com.twitter.finagle.{Filter, Service}
import com.twitter.util.Future

/** A [[Filter]] that unwraps the `Request[_]` and `Response[_]` */
private[finagle] class RequestResponseToReqRepFilter[Req, Rep]
    extends Filter[Request[Req], Response[Rep], Req, Rep] {
  override def apply(
    request: Request[Req],
    service: Service[Req, Rep]
  ): Future[Response[Rep]] = service(request.value).map(Response(_))
}
