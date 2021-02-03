package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag
import com.twitter.jvm.numProcs

/**
 * Flag for defining the number of worker threads used by the Finagle Netty 4 backend. This defaults
 * to 2 x available CPUs but this behavior can be overridden via the auto offloading flag.
 *
 * {{
 *  -com.twitter.finagle.offload.auto=true
 * }}
 *
 * When auto offloading is enabled and no explicit c.t.f.netty.numWorkers is provided, the number of
 * Netty workers defaults to 1/3 of available CPUs with a floor of 4 (on a host with less than
 * 12 CPUs, 4 Netty workers would be allocated).
 *
 * @note For the default value, we set a floor of 8 workers so in the case of constrained
 *       environments we don't end up with detrimentally small worker pool sizes.
 */
object numWorkers
    extends GlobalFlag(
      if (com.twitter.finagle.offload.auto()) math.max(4, (numProcs() / 3).ceil.toInt)
      else math.max(8, (numProcs() * 2).ceil.toInt),
      "The number of netty4 worker threads. This defaults to 2 x CPUs but the behavior can" +
        "be overridden with the com.twitter.finagle.offload.auto=true flag."
    )
