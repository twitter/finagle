package com.twitter.finagle.memcached.loadbalancer

import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.{Addr, Address, Stack, ServiceFactory, Stackable}

/**
 * Exposes a [[StackModule]] which composes over the [[LoadBalancerFactory]] to
 * change the way the way endpoints are created for a load balancer. In particular,
 * each endpoint that is resolved is replicated by the given [[Param]]. This, increases
 * concurrency for each identical endpoint and allows them to be load balanced over. This
 * is useful for pipelining protocols that may incur head-of-line blocking without
 * this replication.
 */
object ConcurrentLoadBalancerFactory {
  private val ReplicaKey = "concurrent_lb_replica"

  // package private for testing
  private[finagle] def replicate(num: Int): Address => Set[Address] = {
    case Address.Inet(ia, metadata) =>
      for (i: Int <- (0 until num).toSet) yield
        Address.Inet(ia, metadata + (ReplicaKey -> i))
    case addr => Set(addr)
  }

  /**
   * A class eligible for configuring the number of connections
   * a single endpoint has.
   */
  case class Param(numConnections: Int) {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(4))
  }

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new LoadBalancerFactory.StackModule[Req, Rep] {
      val description = "Balance requests across multiple connections on a single " +
        "endpoint, used for pipelining protocols"

      override def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val Param(numConnections) = params[Param]
        val LoadBalancerFactory.Dest(dest) = params[LoadBalancerFactory.Dest]
        val newDest = dest.map {
          case bound@Addr.Bound(set, _) =>
            bound.copy(addrs = set.flatMap(replicate(numConnections)))
          case addr => addr
        }
        super.make(params + LoadBalancerFactory.Dest(newDest), next)
      }
    }
}