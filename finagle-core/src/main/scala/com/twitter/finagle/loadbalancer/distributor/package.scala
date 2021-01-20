package com.twitter.finagle.loadbalancer

import com.twitter.util.{Activity, Updatable, Var}

package object distributor {

  /**
   * An intermediate representation of the endpoints that a load balancer
   * operates over, capable of being updated.
   */
  type BalancerEndpoints[Req, Rep] =
    Var[Activity.State[Set[EndpointFactory[Req, Rep]]]]
      with Updatable[
        Activity.State[Set[EndpointFactory[Req, Rep]]]
      ]

}
