package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.aperture.DeterministicAperture.MinDeterministicAperture
import com.twitter.finagle.loadbalancer.aperture.ProcessCoordinate.Coord
import com.twitter.finagle.loadbalancer.exp.restrictZone
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.CoreToggles
import com.twitter.finagle.Status
import scala.util.hashing.MurmurHash3

object WeightedApertureToggle {
  private val toggle = CoreToggles("com.twitter.finagle.loadbalancer.WeightedAperture.v2")
  private val zoneAllowed =
    if (restrictZone()) ServerInfo().zone.getOrElse("") == "smf1" else true

  def apply(client: String): Boolean = {
    toggle(
      MurmurHash3.mix(ServerInfo().clusterId.hashCode, client.hashCode)
    ) && zoneAllowed
  }
}

private object WeightedAperture {

  /**
   * Normalizes `weights` between [0, 1).
   *
   * @note negative weights are not allowed and result in an exception.
   */
  def normalize(weights: Seq[Double]): Seq[Double] = {
    assert(weights.forall(_ > 0), "negative weight not supported")
    val sum = weights.sum
    if (sum == 0) weights.map { _ => 1d / weights.size }
    else weights.map(_ / sum)
  }

  /**
   * Creates a subset of endpoints that exist within a client's aperture by arranging endpoints
   * along a ring bounded [0,1) where their arc-lengths are equivalent to their associated
   * normalized weight (probability). This is accomplished without the actual construction of a
   * [[Ring]]
   *
   * @param weights The complete seq of weights for endpoints a client may send traffic to
   * @param coord The [[Coord]] associated with a given client
   * @return A collection of size `weights.size` of node weights per aperture slice. Nodes outside
   *         the aperture are included but assigned 0 weight so that traffic is not sent to them.
   *
   */
  def adjustWeights(
    weights: IndexedSeq[Double],
    coord: Coord,
    logicalAperture: Int
  ): IndexedSeq[Double] = {

    val numNodes = weights.size
    // The remote width (1 / numNodes) represents the average width of numNodes nodes of variable
    // size. Because the nodes are all normalized, this is a true average. For example, 3 nodes
    // of normalized weights (0.1, 0.1, 0.8) have an average weight of (0.1, 0.1, 0.8) / 3 or 1 / 3
    val apertureWidth =
      DeterministicAperture.dApertureWidth(coord.unitWidth, 1.0 / numNodes, logicalAperture)
    adjustWeights(weights, coord.offset, apertureWidth)
  }

  def adjustWeights(
    weights: IndexedSeq[Double],
    coord: Coord
  ): IndexedSeq[Double] = {
    adjustWeights(weights, coord, math.min(MinDeterministicAperture, weights.size))
  }

  def adjustWeights(
    weights: IndexedSeq[Double],
    offset: Double,
    width: Double
  ): IndexedSeq[Double] = {

    val numNodes = weights.size
    val normalizedWeights = normalize(weights)
    val start = offset
    val end = offset + width

    var position = 0.0
    var idx = 0
    val newWeights = new Array[Double](numNodes)

    // Instead of binary searching the for the start of our slice,
    // we walk the ring from 0 until `end`. When we find nodes inside
    // of our slice, we adjust their normalized weight to fit cleanly
    // within the slice. Nodes that fall outside of our slice are ignored
    // and result in a weight of 0
    while (position < end) {
      // The width of each endpoint is equivalent to its normalized weight
      val idxWidth = normalizedWeights(idx)

      if (position < start) {
        // We're not to the start of our slice yet
        if (position + idxWidth >= start) {
          // We've found the first node in our slice
          // We get the minimum of width and idxWeight+position-start to account for the case
          // where the size of the remote node exceeds the size of the aperture
          newWeights(idx) += math.min(width, idxWidth + position - start)
        }
      } else if (position + idxWidth > end) {
        // If we've reached the final node of our slice, adjust its weight
        newWeights(idx) += end - position
      } else {
        // Otherwise, the entire node is included and its weight is unchanged
        newWeights(idx) += idxWidth
      }

      // Increment our counters
      position += idxWidth
      // Wrap around if necessary
      idx = (idx + 1) % numNodes
    }

    newWeights
  }
}

private class WeightedAperture[Req, Rep, NodeT <: ApertureNode[Req, Rep]](
  protected val aperture: Aperture[Req, Rep] { type Node = NodeT },
  protected val endpoints: Vector[NodeT],
  initAperture: Int,
  protected val coord: Coord)
    extends BaseDist[Req, Rep, NodeT](aperture, endpoints, initAperture) {

  // We don't use aperture.minAperture directly for parity with DeterministicAperture
  override def min: Int = math.min(MinDeterministicAperture, endpoints.size)
  override def logicalAperture =
    if (aperture.minApertureOverride >= 1) aperture.minApertureOverride else min
  def eagerConnections: Boolean = aperture.eagerConnections
  def dapertureActive: Boolean = aperture.dapertureActive

  private[this] val rng = aperture.rng

  //exposed for testing
  private[aperture] val (idxs, pdist) = {
    val weights =
      WeightedAperture.adjustWeights(endpoints.map(_.factory.weight), coord, logicalAperture)

    val indexes = Set.newBuilder[Int]
    val nonZeroWeights = IndexedSeq.newBuilder[Double]
    val nonZeroEndpoints = IndexedSeq.newBuilder[NodeT]

    (weights.indices).foreach { i =>
      if (weights(i) > 0.0) {
        indexes += i
        nonZeroWeights += weights(i)
        nonZeroEndpoints += endpoints(i)
      }
    }
    (indexes.result(), new Alias(nonZeroWeights.result(), rng, nonZeroEndpoints.result()))
  }

  def indices: Set[Int] = idxs
  override def physicalAperture: Int = indices.size

  // copied from DeterministicAperture
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

  override def rebuild(newVector: Vector[NodeT]): This =
    // Although `needsRebuild` is set to false, the Balancer will trigger a rebuild
    // when it exhausts picking busy nodes. Let's explicitly return the same distributor
    // if the coordinates and serverset have not changed.
    ProcessCoordinate() match {
      case Some(newCoord) if (newCoord == coord && newVector == vector) => this
      case _ => super.rebuild(newVector)
    }

  def additionalMetadata: Map[String, Any] = Map(
    "peer_offset" -> coord.offset,
    "peer_unit_width" -> coord.unitWidth,
    "nodes" -> idxs.toSeq.sorted.zipWithIndex.collect {
      case (realIndex, virtualIndex) =>
        Map[String, Any](
          "index" -> realIndex,
          "name_server_weight" -> pdist.get(virtualIndex).factory.weight,
          "weight" -> pdist.weight(virtualIndex),
          "address" -> pdist.get(virtualIndex).factory.toString,
          "status" -> pdist.get(virtualIndex).factory.status.toString
        )
    }
  )

  def needsRebuild: Boolean = false

  def pick(): NodeT = WeightedP2CPick.pick(pdist, aperture.pickLog)
}
