package com.twitter.finagle.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Future, Promise}

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
