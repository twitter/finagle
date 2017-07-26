package com.twitter.finagle.mux.lease.exp

import scala.util.Random

/**
 * A random number generation that is consistent in the same garbage collection.
 * However, if there has been an interstitial garbage collection since the last
 * time the generator was used, the random number will be recomputed.
 */
private[lease] class GenerationalRandom private[lease] (info: JvmInfo, rand: Random) {
  def this(info: JvmInfo) = this(info, new Random())

  private var last = rand.nextInt().abs
  private var gen = info.generation()

  def apply() = synchronized {
    if (gen != info.generation()) {
      gen = info.generation()
      last = rand.nextInt().abs
    }
    last
  }
}
