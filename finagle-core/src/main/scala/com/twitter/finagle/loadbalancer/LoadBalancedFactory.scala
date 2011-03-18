package com.twitter.finagle.loadbalancer

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.Future

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.NoBrokersAvailableException

/**
 * A LoadBalancerStrategy implements a load balancing algorithm. Given
 * a request and a set of service pools, it picks the appropriate
 * pool.
 */
trait LoadBalancerStrategy[Req, Rep]
  extends (Seq[ServiceFactory[Req, Rep]] => Future[Service[Req, Rep]])

// pass in strategy, etc.
class LoadBalancedFactory[Req, Rep](
    factories: Seq[ServiceFactory[Req, Rep]],
    strategy: LoadBalancerStrategy[Req, Rep])
  extends ServiceFactory[Req, Rep]
{
  def make(): Future[Service[Req, Rep]] = {
    // We first create a snapshot since the underlying seq could
    // change.
    val snapshot = factories.toSeq
    if (snapshot.isEmpty)
      return Future.exception(new NoBrokersAvailableException)

    val available = snapshot filter { _.isAvailable }

    // If none are available, we load balance over all of them. This
    // is to remedy situations where the health checking becomes too
    // pessimistic.
    if (available.isEmpty)
      strategy(snapshot)
    else
      strategy(available)
  }

  override def isAvailable = factories.exists(_.isAvailable)

  override def close() = factories foreach { _.close() }
}
