package com.twitter.finagle.loadbalancer

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{Group, NoBrokersAvailableException, ServiceFactory}

abstract class LoadBalancerFactory {
  def newLoadBalancer[Req, Rep](
    group: Group[ServiceFactory[Req, Rep]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]
}
