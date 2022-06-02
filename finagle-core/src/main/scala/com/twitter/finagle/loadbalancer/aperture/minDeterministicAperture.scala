package com.twitter.finagle.loadbalancer.aperture

import com.twitter.app.GlobalFlag

/**
 * Flag for the minimum number of backend instances that deterministic aperture will utilize.
 */
private object minDeterministicAperture
    extends GlobalFlag(
      12,
      "The minimum aperture that the deterministic aperture load balancer will allow"
    )
