package com.twitter.finagle

/**
 * This package implements client side load balancing algorithms.
 *
 * As an end-user, see the [[Balancers]] API to create instances which can be
 * used to configure a Finagle client with various load balancing strategies.
 *
 * As an implementor, each algorithm gets its own subdirectory and is exposed
 * via the [[Balancers]] object. Several convenient traits are provided which factor
 * out common behavior and can be mixed in (i.e. Balancer, DistributorT, NodeT,
 * and Updating).
 */
package object loadbalancer