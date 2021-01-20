package com.twitter.finagle.loadbalancer.distributor

import com.twitter.finagle.ServiceFactory

/**
 * A load balancer and its associated weight. Size refers to the
 * size of the balancers backing collection. The [[Distributor]]
 * operates over these.
 */
case class WeightClass[Req, Rep](
  balancer: ServiceFactory[Req, Rep],
  endpoints: BalancerEndpoints[Req, Rep],
  weight: Double,
  size: Int)
