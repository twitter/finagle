package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.util.{Drv, Rng}

/**
 * Implementation of [[ProbabilityDistribution]] that creates and extracts information
 * from a [[Drv]] for a sequence of weighted outcomes
 *
 * @param weights the probability distribution for a series of outcomes, where each index holds the
 *             probability of selecting that outcome
 * @param endpoints an indexed sequence of the outcomes themselves
 * @tparam T the random variable that the probability distribution represents.
 *
 */
private class Alias[T](weights: IndexedSeq[Double], rng: Rng, endpoints: IndexedSeq[T])
    extends ProbabilityDistribution[T] {
  private[aperture] val drv: Drv = Drv.fromWeights(weights)
  def pickOne(): Int = drv(rng)
  def weight(a: Int): Double = weights(a)
  def get(i: Int): T = endpoints(i)

}
