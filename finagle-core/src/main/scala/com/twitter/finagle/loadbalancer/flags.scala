package com.twitter.finagle.loadbalancer

import com.twitter.app.GlobalFlag

/**
 * NOTE: using any of these flags is generally not encouraged as Finagle processes
 * contain many clients and these configure clients globally per-process. It's
 * encouraged to configure clients individually.
 */
/**
 * A [[GlobalFlag]] that changes the default balancer for every client in the process.
 * Valid choices are ['heap', 'choice', 'aperture', and 'random_aperture'].
 *
 * @note that 'random_aperture' should only be used in unusual situations such as for
 *       testing instances and requires extra configuration. See the aperture
 *       documentation for more information.
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
object perHostStats
    extends GlobalFlag[Boolean](
      false,
      "enable/default per-host stats.\n" +
        "\tWhen enabled,the configured stats receiver will be used,\n" +
        "\tor the loaded stats receiver if none given.\n" +
        "\tWhen disabled, the configured stats receiver will be used,\n" +
        "\tor the NullStatsReceiver if none given."
    )

object useCanonicalHostname
  extends GlobalFlag[Boolean](
    false,
    "enable/default using canonical host name in stats.\n" +
      "\tWhen enabled, canonical host name for endpoint will be used.\n" +
      "\tWhen disabled, given hostname will be used."
  )

package exp {

  import com.twitter.finagle.loadbalancer.aperture.EagerConnectionsType

  object loadMetric
      extends GlobalFlag(
        "leastReq",
        "Metric used to measure load across endpoints (leastReq | ewma)"
      )

  object apertureEagerConnections
      extends GlobalFlag[EagerConnectionsType.Value](
        EagerConnectionsType.Enable,
        "enable aperture eager connections\n" +
          "\tAccepts one of 3 values of EagerConnectionType: Enable, Disable, and ForceWithDtab.\n" +
          "\tWhen enabled, the aperture load balancer will eagerly establish connections with\n" +
          "\tall endpoints in the aperture. This does not typically apply for hosts resolved in\n" +
          "\tresult of request-level dtab overrides unless set to ForceWithDtab."
      )

  object restrictZone
      extends GlobalFlag[Boolean](
        true,
        "enable zone restriction\n" +
          "When enabled, restricts loadbalancer toggles to a single zone. This applies only to \n" +
          "zone-specific toggles, such as com.twitter.finagle.loadbalancer.WeightedAperture. \n" +
          "Enabled by default."
      )

}
