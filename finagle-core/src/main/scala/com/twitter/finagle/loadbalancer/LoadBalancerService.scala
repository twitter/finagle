package com.twitter.finagle.loadbalancer

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.Future

import com.twitter.finagle.Service
import com.twitter.finagle.NoBrokersAvailableException

/**
 * A LoadBalancerStrategy implements a load balancing algorithm. Given
 * a request and a set of services, it is responsible for dispatching
 * the request to an appropriate host.
 */
trait LoadBalancerStrategy[Req, Rep] {
  /**
   * dispatch a the given request to one of the underlying
   * services. Upon succesful dispatch, returns Some(service,
   * replyFuture). If the dispatch cannot be made, returns None.
   */
  def dispatch(
    request: Req,
    services: Seq[Service[Req, Rep]]): Option[(Service[Req, Rep], Future[Rep])]
}

/**
 * The LoadBalancerService is a service that utilizes a load balancing
 * strategy in order to dispatch a given request.
 */
class LoadBalancerService[-Req, +Rep](
  services: Seq[Service[Req, Rep]],
  strategy: LoadBalancerStrategy[Req, Rep])
  extends Service[Req, Rep]
{
  def apply(request: Req) = {
    strategy.dispatch(request, services) match {
      case Some((_, future)) => future
      case None => Future.exception(new NoBrokersAvailableException)
    }
  }
}
