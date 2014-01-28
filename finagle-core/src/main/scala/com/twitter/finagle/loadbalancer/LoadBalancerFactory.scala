package com.twitter.finagle.loadbalancer

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{Group, NoBrokersAvailableException, ServiceFactory}
import com.twitter.util.Var
import com.twitter.app.GlobalFlag
import java.util.logging.{Level, Logger}

object defaultBalancer extends GlobalFlag("heap", "Default load balancer")

trait WeightedLoadBalancerFactory {
  def newLoadBalancer[Req, Rep](
    weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]
}

abstract class LoadBalancerFactory {
  def newLoadBalancer[Req, Rep](
    group: Group[ServiceFactory[Req, Rep]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]

  def toWeighted: WeightedLoadBalancerFactory = new WeightedLoadBalancerFactory {
    def newLoadBalancer[Req, Rep](
        weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
        statsReceiver: StatsReceiver,
        emptyException: NoBrokersAvailableException) = {
      val unweighted = weighted map { set =>
        set map { case (f, _) => f }
      }
      LoadBalancerFactory.this.newLoadBalancer(
        Group.fromVar(unweighted), statsReceiver, emptyException)
    }
  }
}

object DefaultBalancerFactory extends WeightedLoadBalancerFactory {
  val underlying =
    defaultBalancer() match {
      case "choice" => P2CBalancerFactory
      case "heap" => HeapBalancerFactory.toWeighted
      case x =>
        Logger.getLogger("finagle").log(Level.WARNING, 
          "Invalid load balancer %s, using balancer \"heap\"".format(x))
        HeapBalancerFactory.toWeighted
    }

  def newLoadBalancer[Req, Rep](
    weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep] =
      underlying.newLoadBalancer(weighted, statsReceiver, emptyException)
}
