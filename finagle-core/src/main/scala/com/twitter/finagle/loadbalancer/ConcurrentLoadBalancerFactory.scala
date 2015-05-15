package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.util.Activity
import java.net.SocketAddress
import java.util.logging.Level

/**
 * A load balancer that balances among multiple connections,
 * useful for managing concurrency in pipelining protocols.
 *
 * Each endpoint can open multiple connections. For N endpoints,
 * each opens M connections, load balancer balances among N*M
 * options. Thus, it increases concurrency of each endpoint.
 */
private[finagle] object ConcurrentLoadBalancerFactory {
  import LoadBalancerFactory._

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

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new BalancerStackModule[Req, Rep] {
      val description = "Balance requests across multiple connections on a single endpoint, used for pipelining protocols"
      val parameters = Seq(
        implicitly[Stack.Param[ErrorLabel]],
        implicitly[Stack.Param[Dest]],
        implicitly[Stack.Param[param.Stats]],
        implicitly[Stack.Param[param.Logger]],
        implicitly[Stack.Param[param.Monitor]],
        implicitly[Stack.Param[param.Reporter]],
        implicitly[Stack.Param[Param]])

      override protected def processAddrs(
        params: Stack.Params,
        addrs: Set[SocketAddress]
      ): Set[SocketAddress] = {
        val n = params[Param].numConnections
        addrs.flatMap(SocketAddresses.replicate(n))
      }
    }
}