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
  // With an average aperture of ~20, the probability of selecting the same node 5 times is
  // 1/3200,000 for equally weighted nodes. For a worse-case scenario, selecting
  // the same node 5 times if that node is the most heavily weighted, with normalized weight of say
  // 19/20, has a probability of only 70% -- since we'd like to send the majority of traffic to
  // the heavily weighted node anyway, we're ok with that probability.
  private[this] final val maxIter = 5
  private[aperture] val drv: Drv = Drv.fromWeights(weights)
  def pickOne(): Int = drv(rng)
  def tryPickSecond(a: Int): Int = {
    var b = a
    var i = 0
    while (a == b && i < maxIter) {
      b = pickOne()
      i = i + 1
    }
    b
  }
  def weight(a: Int): Double = weights(a)
  def get(i: Int): T = endpoints(i)

}
