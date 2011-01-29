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
    pools: Seq[ServiceFactory[Req, Rep]],
    strategy: LoadBalancerStrategy[Req, Rep])
  extends ServiceFactory[Req, Rep]
{
  def make() = {
    // Make a snapshot of the pools. The passed-in seqs may not be
    // immutable.
    val snapshot = pools.toArray filter { _.isAvailable }

    // TODO: (XXX) include *every* service if none are available.
    
    if (snapshot.isEmpty)
      Future.exception(new NoBrokersAvailableException)
    else
      strategy(snapshot)
  }

  override def isAvailable = pools.exists(_.isAvailable)

  override def release() = pools foreach { _.release() }
}
