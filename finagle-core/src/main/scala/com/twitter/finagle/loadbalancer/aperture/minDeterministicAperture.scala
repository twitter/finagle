package com.twitter.finagle.loadbalancer.aperture

import com.twitter.app.GlobalFlag

// This is for initial tuning only and will be removed
private object minDeterministicAperture
    extends GlobalFlag(
      12,
      "The minimum aperture that the deterministic aperture load balancer will allow"
    )
