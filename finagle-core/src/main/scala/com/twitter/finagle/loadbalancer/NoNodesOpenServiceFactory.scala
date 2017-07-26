package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.util.Future

/**
 * Used by [[LoadBalancerFactory]] when configured with
 * [[WhenNoNodesOpen.FailFast]].
 */
private class NoNodesOpenServiceFactory[Req, Rep](underlying: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy[Req, Rep](underlying) {

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (underlying.status != Status.Open)
      // recreate an exception each time in order to have
      // a more meaningful stacktrace
      Future.exception(new NoNodesOpenException(FailureFlags.NonRetryable))
    else
      underlying(conn)
  }

}
