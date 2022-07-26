package com.twitter.finagle.loadbalancer.aperture

import com.twitter.logging.Logger

object DeterministicAperture {
  private[this] val log = Logger.get()

  /**
   * Compute the width of the aperture slice using the logical aperture size and the local
   * and remote ring unit widths.
   */
  private[loadbalancer] def dApertureWidth(
    localUnitWidth: Double,
    remoteUnitWidth: Double,
    logicalAperture: Int
  ): Double = {
    // A recasting of the formula
    // clients*aperture <= N*servers
    // - N is the smallest integer satisfying the inequality and represents
    //   the number of times we have to circle the ring.
    // -> ceil(clients*aperture/servers) = N
    // - unitWidth = 1/clients; ring.unitWidth = 1/servers
    // -> ceil(aperture*ring.unitWidth/unitWidth) = N
    val unitWidth: Double = localUnitWidth // (0, 1.0]

    val unitAperture: Double = logicalAperture * remoteUnitWidth // (0, 1.0]
    val N: Int = math.ceil(unitAperture / unitWidth).toInt
    val width: Double = N * unitWidth
    // We know that `width` is bounded between (0, 1.0] since `N`
    // at most will be the inverse of `unitWidth` (i.e. if `unitAperture`
    // is 1, then units = 1/(1/x) = x, width = x*(1/x) = 1). However,
    // practically, we take the min of 1.0 to account for any floating
    // point stability issues.
    math.min(1.0, width)
  }

  // When picking a min aperture, we want to ensure that p2c can actually converge
  // when there are weights present. Based on empirical measurements, weights are well
  // respected when we have 4 or more servers.
  // The root of the problem is that you can't send a fractional request to the (potentially)
  // fractionally weighted edges of the aperture. The following thought experiment illustrates
  // this.
  // First, we consider the limiting case of only one weighted node. If we only have one node
  // to choose from, it's impossible to respect the weight since we will always return the
  // single node – we need at least 2 nodes in this case.
  // Next, we extend the thought experiment to the case of pick2. How does the probability of
  // picking the second node change? Consider the case of 3 nodes of weights [1, 1, 0.5]. The
  // probability of node 2 being picked on the first try is 0.5/2.5, but it changes for the
  // second pick to 0.5/1.5. This shifting of probability causes a drift in the probability
  // of a node being either of the two picked and in the case of the three nodes above, the
  // probability of being picked either first or second is ~0.61 relative to nodes 0 or 1,
  // meaningfully different than the desired value of 0.50.
  // Next, we extrapolate this to the case of a large number of nodes. As the number of nodes
  // in the aperture increases the numerator (a node's weight) of the probability stays the same
  // but denominator (the sum of weights) increases. As N reaches infinity, the difference in
  // probability between being picked first or second converges to 0, restoring the probabilities
  // to what we expect. Running the same simulation with N nodes where the last node has 0.5
  // weight results in the following simulated probabilities (P) relative to nodes with weight 1
  // of picking the last node (weight 0.5) for either the first or second pick:
  //      N     2       3       4       6      10     10000
  //      P    1.0    0.61    0.56    0.53    0.52     0.50
  // While 4 healthy nodes has been determined to be sufficient for the p2c picking algorithm,
  // it is susceptible to finding it's aperture without any healthy nodes. While this is rare
  // in isolation it becomes more likely when there are many such sized apertures present.
  // Therefore, we've assigned the min to 12 to further decrease the probability of having a
  // aperture without any healthy nodes.
  // Note: the flag will be removed and replaced with a constant after tuning.
  private[loadbalancer] val MinDeterministicAperture: Int = {
    val min = minDeterministicAperture()
    if (1 < min) min
    else {
      log.warning(
        s"Unexpectedly low minimum d-aperture encountered: $min. " +
          s"Check your configuration. Defaulting to 12."
      )
      12
    }
  }
}
