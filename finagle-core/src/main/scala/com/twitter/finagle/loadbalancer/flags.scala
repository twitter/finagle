package com.twitter.finagle.loadbalancer

import com.twitter.app.GlobalFlag

/**
 * NOTE: using any of these flags is generally not encouraged as Finagle processes
 * contain many clients and these configure clients globally per-process. It's
 * encouraged to configure clients individually.
 */

/**
 * A [[GlobalFlag]] that changes the default balancer for every client in the process.
 * Valid choices are ['heap', 'choice', and 'aperture'].
 *
 * To configure the load balancer on a per-client granularity instead, use the
 * `withLoadBalancer` method like so:
 *
 * {{
 *    val balancer = Balancers.aperture(...)
 *    $Protocol.client.withLoadBalancer(balancer)
 * }}
 */
object defaultBalancer extends GlobalFlag("choice", "Default load balancer")

/**
 * A [[GlobalFlag]] which allows the configuration of per host (or endpoint)
 * stats to be toggled. Note, these are off by default because they tend
 * to be expensive, especially when the size of the destination cluster
 * is large. However, they can be quite useful for debugging.
 */
object perHostStats extends GlobalFlag(false, "enable/default per-host stats.\n" +
  "\tWhen enabled,the configured stats receiver will be used,\n" +
  "\tor the loaded stats receiver if none given.\n" +
  "\tWhen disabled, the configured stats receiver will be used,\n" +
  "\tor the NullStatsReceiver if none given.")

package exp {
  object loadMetric extends GlobalFlag("leastReq",
    "Metric used to measure load across endpoints (leastReq | ewma)")
}