package com.twitter.finagle.loadbalancer.distributor

import com.twitter.finagle.ServiceFactory

/**
 * Represents cache entries for load balancer instances. Stores both
 * the load balancer instance and its backing updatable collection.
 * Size refers to the number of elements in `endpoints`.
 */
case class CachedBalancer[Req, Rep](
  balancer: ServiceFactory[Req, Rep],
  endpoints: BalancerEndpoints[Req, Rep],
  size: Int)
