package com.twitter.finagle.client

import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.util.StackRegistry
import com.twitter.finagle.{Addr, param, Stack}
import com.twitter.util.Future
import java.util.logging.Level

private[twitter] object ClientRegistry extends StackRegistry {
  /**
   * Get a Future which is satisfied when the dest of every currently
   * registered client is no longer pending.
   * @note The destination of clients may later change to pending.
   * @note Clients are only registered after creation (i.e. calling `newClient` or
   *       `build` with ClientBuilder APIs).
   * @note Experimental feature which will eventually be solved by exposing Service
   *       availability as a Var.
   */
  def expAllRegisteredClientsResolved(): Future[Set[String]] = synchronized {
    val fs = registrants map { case StackRegistry.Entry(name, _, _, params) =>
      val LoadBalancerFactory.Dest(va) = params[LoadBalancerFactory.Dest]
      val param.Logger(log) = params[param.Logger]

      val resolved = va.changes.filter(_ != Addr.Pending).toFuture
      resolved map { resolution =>
        log.info(s"${name} params ${params}")
        log.info(s"${name} resolved to ${resolution}")
        name
      }
    }

    Future.collect(fs.toSeq).map(_.toSet)
  }
}
