package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag
import com.twitter.jvm.numProcs

/**
 * Flag for defining the number of worker threads used by the Finagle Netty 4 backend
 *
 * @note For the default value, we set a floor of 8 workers so in the case of constrained
 *       environments we don't end up with detrimentally small worker pool sizes.
 */
object numWorkers
    extends GlobalFlag(math.max(8, (numProcs() * 2).ceil.toInt), "number of netty4 worker threads")
