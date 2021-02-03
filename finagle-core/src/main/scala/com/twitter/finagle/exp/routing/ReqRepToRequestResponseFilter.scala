package com.twitter.finagle.exp.routing

import com.twitter.finagle.{Filter, Service}
import com.twitter.util.Future

/**
 * A utility for starting the Request[_], Response[_] chain
 * with a service that doesn't need access to the Request[_]/Response[_] message envelopes.
 */
private[finagle] class ReqRepToRequestResponseFilter[Req, Rep]
    extends Filter[Req, Rep, Request[Req], Response[Rep]] {
  override def apply(
    request: Req,
    service: Service[Request[Req], Response[Rep]]
  ): Future[Rep] = service(Request(request)).map(_.value)
}
