package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.aperture.ProcessCoordinate.Coord
import com.twitter.finagle.Address
import com.twitter.finagle.Status
import com.twitter.logging.Level
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
    val min = minDeterminsticAperture()
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

/**
 * [[DeterministicAperture]] addresses the shortcomings of [[RandomAperture]] by picking
 * nodes within this process' [[ProcessCoordinate]]. Thus, when the group of peers
 * converges on an aperture size, the servers are equally represented across the
 * peers.
 *
 * @param vector The source vector received from a call to `rebuild`.
 * @param initAperture The initial aperture to use.
 * @param coord The [[ProcessCoordinate]] for this process which is used to narrow
 * the range of `pick2`.
 */
private final class DeterministicAperture[Req, Rep, NodeT <: ApertureNode[Req, Rep]](
  aperture: Aperture[Req, Rep] { type Node = NodeT },
  vector: Vector[NodeT],
  initAperture: Int,
  coord: Coord)
    extends BaseDist[Req, Rep, NodeT](
      aperture,
      vector,
      initAperture
    ) {
  import DeterministicAperture._
  require(vector.nonEmpty, "vector must be non empty")

  private[this] val labelForLogging = aperture.lbl
  private[this] val rng = aperture.rng

  def eagerConnections: Boolean = aperture.eagerConnections

  def dapertureActive: Boolean = aperture.dapertureActive

  def minAperture: Int = aperture.minAperture

  def minApertureOverride: Int = aperture.minApertureOverride

  private[this] val ring = new Ring(vector.size, rng)

  private[this] val pdist = new ProbabilityDistribution[NodeT] {
    def pickOne(): Int = ring.pick(coord.offset, apertureWidth)
    override def tryPickSecond(a: Int): Int = ring.tryPickSecond(a, coord.offset, apertureWidth)
    def weight(a: Int): Double = ring.weight(a, coord.offset, apertureWidth)
    def get(i: Int): NodeT = vector(i)
  }

  def pick(): NodeT = WeightedP2CPick.pick(pdist, aperture.pickLog)

  // Note that this definition ignores the user defined `minAperture`,
  // but that isn't likely to hold much value given our definition of `min`
  // and how we calculate the `apertureWidth`.
  override def min: Int = math.min(MinDeterministicAperture, vector.size)

  // DeterministicAperture does not dynamically adjust the aperture based on load
  override def logicalAperture: Int = {
    if (minApertureOverride >= 1) minApertureOverride else min
  }

  // Translates the logical `aperture` into a physical one that
  // maps to the ring. Note, we do this in terms of the peer
  // unit width in order to ensure full ring coverage. As such,
  // this width will be >= the aperture. Put differently, we may
  // cover more servers than the `aperture` requested in service
  // of global uniform load.
  private[this] def apertureWidth: Double =
    dApertureWidth(coord.unitWidth, ring.unitWidth, logicalAperture)

  override def physicalAperture: Int = {
    val width = apertureWidth
    if (aperture.rebuildLog.isLoggable(Level.DEBUG)) {
      aperture.rebuildLog.debug(
        f"[DeterministicAperture.physicalAperture $labelForLogging] ringUnit=${ring.unitWidth}%1.6f coordUnit=${coord.unitWidth}%1.6f coordOffset=${coord.offset}%1.6f apertureWidth=$width%1.6f"
      )
    }
    ring.range(coord.offset, width)
  }

  override val indices: Set[Int] = ring.indices(coord.offset, apertureWidth).toSet

  private[this] def nodes: Seq[(Int, Double, Address, Status)] = {
    indices.toSeq.sorted.map { i =>
      val factory = vector(i).factory
      val addr = factory.address
      val weight = ring.weight(i, coord.offset, apertureWidth)
      val status = factory.status
      (i, weight, addr, status)
    }
  }

  // Save an array-version of the indices that we can traverse over index-wise for
  // a zero allocation `status` implementation.
  //
  // DeterministicAperture has `coord` and `logicalAperture` fixed on construction,
  // used to compute `indices`. We can safely cache the resulting set into `seqIndices`.
  // This cache will be recomputed on subsequent rebuilds.
  private[this] val seqIndices: Array[Int] = indices.toArray
  def status: Status = {
    var i = 0
    var status: Status = Status.Closed
    while (i < seqIndices.length && status != Status.Open) {
      status = Status.best(status, vector(seqIndices(i)).factory.status)

      i += 1
    }

    status
  }

  // We log the contents of the aperture on each distributor rebuild when using
  // deterministic aperture. Rebuilds are not frequent and concentrated around
  // events where this information would be valuable (i.e. coordinate changes or
  // host add/removes).
  if (aperture.rebuildLog.isLoggable(Level.DEBUG)) {
    val apertureSlice: String = {
      nodes
        .map {
          case (i, weight, addr, status) =>
            f"(index=$i, weight=$weight%1.6f, addr=$addr, status=$status)"
        }.mkString("[", ", ", "]")
    }
    aperture.rebuildLog.debug(
      s"[DeterministicAperture.rebuild $labelForLogging] nodes=$apertureSlice")

    // It may be useful see the raw server vector for d-aperture since we expect
    // uniformity across processes.
    if (aperture.rebuildLog.isLoggable(Level.TRACE)) {
      val vectorString = vector.map(_.factory.address).mkString("[", ", ", "]")
      aperture.rebuildLog.trace(
        s"[DeterministicAperture.rebuild $labelForLogging] nodes=$vectorString")
    }
  }

  // rebuilds only need to happen when we receive ring updates (from
  // the servers or our coordinate changing).
  def needsRebuild: Boolean = false

  // Although `needsRebuild` is set to false, the Balancer will trigger a rebuild
  // when it exhausts picking busy nodes. Let's explicitly return the same distributor
  // if the coordinates and serverset have not changed.
  override def rebuild(newVector: Vector[NodeT]): This =
    ProcessCoordinate() match {
      case Some(newCoord) =>
        if (newCoord == coord && newVector == vector) this
        else super.rebuild(newVector)
      case None => super.rebuild(newVector)
    }

  def additionalMetadata: Map[String, Any] = Map(
    "ring_unit_width" -> ring.unitWidth,
    "peer_offset" -> coord.offset,
    "peer_unit_width" -> coord.unitWidth,
    "aperture_width" -> apertureWidth,
    "nodes" -> nodes.map {
      case (i, weight, addr, status) =>
        Map[String, Any](
          "index" -> i,
          "weight" -> weight,
          "address" -> addr.toString,
          "status" -> status.toString
        )
    }
  )
}
