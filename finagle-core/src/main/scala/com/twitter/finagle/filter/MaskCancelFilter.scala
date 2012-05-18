package com.twitter.finagle.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Future, Promise}

/**
 * Prevent cancellations from propagating to the underlying service.
 */
class MaskCancelFilter[Req, Rep] extends SimpleFilter[Req, Rep] {
  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val p = new Promise[Rep]
    service(req).proxyTo(p)
    p
  }
}
