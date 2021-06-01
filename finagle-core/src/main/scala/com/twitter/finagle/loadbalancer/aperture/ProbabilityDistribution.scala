package com.twitter.finagle.loadbalancer.aperture

/**
 *  Representation of the probability distribution of a given random
 *  variable, T. Outcomes within the distribution should be organized
 *  through indexing.
 *
 * @tparam T the random variable that the probability distribution
 *           represents.
 */
private trait ProbabilityDistribution[T] {

  // With an average aperture of ~20, the probability of selecting the same node 5 times is
  // 1/3,200,000 for equally weighted nodes. For a worse-case scenario, selecting
  // the same node 5 times if that node is the most heavily weighted, with normalized weight of say
  // 19/20, has a probability of only 70% -- since we'd like to send the majority of traffic to
  // the heavily weighted node anyway, we're ok with that probability.
  private[this] final val maxIter = 5

  /**
   * Selects an outcome from the Probability distribution
   *
   * @return The index of the chosen random variable
   */
  def pickOne(): Int

  /**
   * Selects an outcome from the Probability distribution
   *
   * @param a the index of the first selected outcome, for choosing without replacement
   * @return The index of the second selected outcome
   */
  def tryPickSecond(a: Int): Int = {
    var b = a
    var i = 0
    while (a == b && i < maxIter) {
      b = pickOne()
      i = i + 1
    }
    b
  }

  /**
   * Retrieves the probability or weight of a selected outcome
   *
   * @param i the index of the outcome whose probability you'd like to query
   * @return the weight or probability associated with that outcome
   */
  def weight(i: Int): Double

  /**
   * Returns the actual random variable outcome, of type T, from its index
   *
   * @param i index of the outcome
   * @return The outcome itself
   */
  def get(i: Int): T
}
