package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Status
import com.twitter.finagle.loadbalancer.p2c.P2CPick
import com.twitter.finagle.util.Rng
import com.twitter.logging.Level
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer

private object RandomAperture {
  private def nodeToken[Req, Rep](node: ApertureNode[Req, Rep]): Int = node.token

  private def nodeOpen[Req, Rep](node: ApertureNode[Req, Rep]): Boolean =
    node.status == Status.Open

  private def nodeBusy[Req, Rep](node: ApertureNode[Req, Rep]): Boolean =
    node.status == Status.Busy

  /**
   * Returns a new vector which is ordered by a node's status. Note, it is
   * important that this is a stable sort since we care about the source order
   * of `vec` to eliminate any unnecessary resource churn.
   */
  private def statusOrder[Req, Rep, Node <: ApertureNode[Req, Rep]](
    vec: Vector[Node]
  ): Vector[Node] = {
    val resultNodes = new VectorBuilder[Node]
    val busyNodes = new ListBuffer[Node]
    val closedNodes = new ListBuffer[Node]

    val iter = vec.iterator
    while (iter.hasNext) {
      val node = iter.next()
      node.status match {
        case Status.Open => resultNodes += node
        case Status.Busy => busyNodes += node
        case Status.Closed => closedNodes += node
      }
    }

    resultNodes ++= busyNodes ++= closedNodes
    resultNodes.result
  }

  private[aperture] final class RandomApertureProbabilityDist[
    Req,
    Rep,
    Node <: ApertureNode[Req, Rep]
  ](
    vec: Vector[Node],
    logicalAperture: () => Int,
    rng: Rng)
      extends ProbabilityDistribution[Node] {
    assert(logicalAperture() <= vec.size)
    // An array of the cumulative probabilities normalized such that their sum is 1.0
    // exposed for testing
    private[aperture] val cumulativeProbability: Array[Double] = {
      val totalProbability = vec.iterator.map(_.factory.weight).sum
      val result = new Array[Double](vec.size)
      var acc = 0.0
      var i = 0

      while (i < vec.size) {
        acc += (vec(i).factory.weight / totalProbability)
        result(i) = acc
        i += 1
      }
      result
    }
    private[aperture] def scaledAperture: Double = {
      assert(logicalAperture() <= vec.size)
      logicalAperture().toDouble / vec.size
    }

    def pickOne(): Int = {
      // We need to scale our window to the logical aperture size while making sure
      // it's limited to the number of elements we have in our logicalAperture window.
      // Unfortunately, that window is in terms of indexes so we have to do some
      // conversion.
      val p = rng.nextDouble() * scaledAperture
      binSearch(p)
    }

    def weight(i: Int): Double = vec(i).factory.weight

    def get(i: Int): Node = vec(i)

    def maxIdx: Int = binSearch(scaledAperture)

    private[this] def binSearch(value: Double): Int = {
      val i = java.util.Arrays.binarySearch(cumulativeProbability, value)
      // The case of an exact match. Should be rare.
      // For a non exact match the binarySearch method returns
      // (-(insertion point) - 1) so we have to invert that
      // to figure out what that insertion point would be, which is
      // just our index. See `j.u.Arrays.binarySearch` for come context.

      // We have to do the math.min to deal with floating point rounding errors:
      // we really don't want to overflow the index.
      if (i >= 0) i else Math.min(cumulativeProbability.size - 1, -(i + 1))
    }
  }
}

/**
 * A distributor which uses P2C to select nodes from within a window ("aperture").
 * The `vector` is shuffled randomly to ensure that clients talking to the same
 * set of nodes don't concentrate load on the same set of servers. However, there is
 * a known limitation with the random shuffle since servers still have a well
 * understood probability of being selected as part of an aperture (i.e. they
 * follow a binomial distribution).
 *
 * @param vector The source vector received from a call to `rebuild`.
 * @param initAperture The initial aperture to use.
 */
private final class RandomAperture[Req, Rep, NodeT <: ApertureNode[Req, Rep]](
  aperture: Aperture[Req, Rep] { type Node = NodeT },
  vector: Vector[NodeT],
  initAperture: Int)
    extends BaseDist[Req, Rep, NodeT](aperture, vector, initAperture) {
  require(vector.nonEmpty, "vector must be non empty")
  import RandomAperture._

  def dapertureActive: Boolean = aperture.dapertureActive

  def eagerConnections: Boolean = aperture.eagerConnections

  def minAperture: Int = aperture.minAperture

  private[this] val labelForLogging = aperture.lbl

  // Since we don't have any process coordinate, we sort the node
  // by `token` which is deterministic across rebuilds but random
  // globally, since `token` is assigned randomly per process
  // when the node is created.
  protected val vec: Vector[NodeT] = statusOrder[Req, Rep, NodeT](vector.sortBy(nodeToken))

  private[this] def vecAsString: String =
    vec
      .take(logicalAperture)
      .map(_.factory.address)
      .mkString("[", ", ", "]")

  if (aperture.rebuildLog.isLoggable(Level.DEBUG)) {
    aperture.rebuildLog.debug(s"[RandomAperture.rebuild $labelForLogging] nodes=$vecAsString")
  }

  def indices: Set[Int] = {
    val range = if (pdist.isEmpty) (0 until logicalAperture) else (0 to pdist.get.maxIdx)
    range.toSet
  }

  // we iterate over the entire set as RandomAperture has the ability for
  // the nodes within its logical aperture to change dynamically. `needsRebuild`
  // captures some of this behavior automatically. However, a circuit breaker sitting
  // above this balancer using `status` as a health check can shadow `needsRebuild`
  // that's checked in `Balancer.apply`.
  def status: Status = {
    var i = 0
    var status: Status = Status.Closed
    while (i < vec.length && status != Status.Open) {
      status = Status.best(status, vec(i).factory.status)

      i += 1
    }

    status
  }

  // This is only necessary for aperture.manageWeights == true
  private[aperture] val pdist: Option[RandomApertureProbabilityDist[Req, Rep, NodeT]] = {
    if (aperture.manageWeights)
      Some(
        new RandomApertureProbabilityDist[Req, Rep, NodeT](
          vec,
          () => logicalAperture,
          aperture.rng))
    else None
  }

  def pick(): NodeT = {
    if (vector.isEmpty) aperture.failingNode
    else if (vector.length == 1) vector(0)
    else if (pdist.isDefined) WeightedP2CPick.pick(pdist.get, aperture.pickLog)
    else P2CPick.pick(vec, logicalAperture, aperture.rng)
  }

  // To reduce the amount of rebuilds needed, we rely on the probabilistic
  // nature of p2c pick. That is, we know that only when a significant
  // portion of the underlying vector is unavailable will we return an
  // unavailable node to the layer above and trigger a rebuild. We do however
  // want to return to our "stable" ordering as soon as we notice that a
  // previously busy node is now available.
  private[this] val busy = vector.filter(nodeBusy)
  def needsRebuild: Boolean = busy.exists(nodeOpen)

  def additionalMetadata: Map[String, Any] = Map("nodes" -> vecAsString)
}
