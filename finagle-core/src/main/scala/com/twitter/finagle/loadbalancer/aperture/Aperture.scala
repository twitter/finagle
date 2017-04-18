package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.p2c.P2CPick
import com.twitter.finagle.loadbalancer.{Balancer, DistributorT, NodeT}
import com.twitter.finagle.util.Rng
import com.twitter.util.{Future, Time}
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer

private object Aperture {
  val nodeToken: NodeT[_, _] => Int = { node => node.token }
  val openNode: NodeT[_, _] => Boolean = { node => node.status == Status.Open }
}

/**
 * The aperture distributor balances load onto a window, the aperture, of
 * underlying capacity. The distributor exposes a control mechanism so that a
 * controller can adjust the aperture according to load conditions.
 *
 * The window contains a number of discrete serving units, one for each
 * node. No load metric is prescribed: this can be mixed in separately.
 *
 * The underlying nodes are arranged in a consistent fashion: an
 * aperture of a given size always refers to the same set of nodes; a
 * smaller aperture to a subset of those nodes so long as the nodes are of
 * equal `status` (i.e. unhealthy nodes are de-prioritized). Thus, it is
 * relatively harmless to adjust apertures frequently, since underlying nodes
 * are typically backed by pools, and will be warm on average.
 */
private[loadbalancer] trait Aperture[Req, Rep] extends Balancer[Req, Rep] { self =>
  import Aperture._
  import DeterministicOrdering._

  /**
   * The random number generator used to pick two nodes for
   * comparison – since aperture uses p2c for selection.
   */
  protected def rng: Rng

  /**
   * The minimum allowable aperture. Must be greater than zero.
   */
  protected def minAperture: Int

  /**
   * Enables [[Aperture]] to read coordinate data from [[DeterministicOrdering]]
   * to derive an ordering for the endpoints used by this [[Balancer]] instance.
   */
  protected def useDeterministicOrdering: Boolean

  /**
   * Adjust the aperture by `n` serving units.
   */
  protected def adjust(n: Int): Unit = invoke(_.adjust(n))

  /**
   * Widen the aperture by one serving unit.
   */
  protected def widen(): Unit = adjust(1)

  /**
   * Narrow the aperture by one serving unit.
   */
  protected def narrow(): Unit = adjust(-1)

  /**
   * The current aperture. This is never less than 1, or more
   * than `units`.
   */
  protected def aperture: Int = dist.aperture

  /**
   * The number of available serving units.
   * The maximum aperture size.
   */
  protected def units: Int = dist.units

  private[this] val gauges = Seq(
    statsReceiver.addGauge("aperture") { aperture },
    statsReceiver.addGauge("use_deterministic_ordering") {
      if (useDeterministicOrdering) 1F else 0F
    }
  )

  private[this] val coordinateUpdates = statsReceiver.counter("coordinate_updates")

  private[this] val coordObservation = DeterministicOrdering.changes.respond { _ =>
    // One nice side-effect of deferring to the balancers `updater` is
    // that we serialize and collapse concurrent updates. So if we have a volatile
    // source that is updating the coord, we are resilient to that. We could
    // go even further by rate limiting the changes if we need to.
    coordinateUpdates.incr()
    self.rebuild()
  }

  /**
   * A distributor that uses P2C to select nodes from within a window ("aperture").
   *
   * @param vector The ordered collection over which the aperture is applied
   * and p2c selects over.
   *
   * @param original The original vector before any ordering is applied.
   * This is necessary to keep intact since the updates we receive from the
   * Balancer apply a specific ordering to the collection of nodes.
   *
   * @param busy The nodes which have been shuffled to the back of the collection
   * because they are considered busy as per their `status`.
   *
   * @param coordinate The last sample read from [[DeterministicOrdering]] that
   * the distributor used.
   *
   * @param initAperture The initial aperture to use.
   */
  protected class Distributor(
      vector: Vector[Node],
      original: Vector[Node],
      busy: Vector[Node],
      coordinate: Option[Coord],
      initAperture: Int)
    extends DistributorT[Node](vector)
    with P2CPick[Node] {

    type This = Distributor

    private[this] val max: Int = vector.size
    private[this] val min: Int = {
      val default = math.min(minAperture, vector.size)
      if (!useDeterministicOrdering) default else {
        coordinate match {
          // We want to additionally ensure that we get full ring coverage
          // when there are fewer clients than servers. For example, imagine the
          // degenerate case where we have a min aperture of size 1 and fewer
          // clients than servers – we know that we will at most cover `size` of
          // of the `vector.size` server ring. Thus, we need an aperture size
          // of `vector.size` / `size`.
          case Some(FromInstanceId(_, _, size)) if size < vector.size =>
            val minSize: Double = vector.size / size.toDouble
            // Since `minSize` can be fractional we do our best to approximate
            // the size of the min size needed to cover the entire server ring.
            // Technically, we could make this "perfect" by taking the fractional
            // bit and translating that to a percentage of the peer group ceiling
            // the value and the remainder flooring it, but we avoid the added
            // complexity by just ceiling for everyone.
            math.max(default, math.ceil(minSize).toInt)

          case _ => default
        }
      }
    }

    // We are guaranteed that writes to aperture are serialized since
    // we only expose them via the narrow, widen, etc. methods above. Those
    // defer to the balancers `updater` which is serial. Therefore, we only
    // need to guarantee visibility across threads and don't need to
    // provide other synchronization between threads.
    @volatile private[this] var _aperture: Int = initAperture
    // Make sure the aperture is within bounds [minAperture, maxAperture].
    adjust(0)

    /**
     * Returns the number of available serving units.
     */
    def units: Int = max

    /**
     * Returns the current aperture.
     */
    def aperture: Int = _aperture

    /**
     * Adjusts the aperture by `n`.
     */
    def adjust(n: Int): Unit = {
      _aperture = math.max(min, math.min(max, _aperture + n))
    }

    protected def rng: Rng = self.rng
    protected def bound: Int = aperture
    protected def emptyNode = failingNode(emptyException)

    def rebuild(): This = rebuild(original)

    /**
     * Returns a new vector with the nodes sorted by `token` which is
     * deterministic across rebuilds but random globally, since `token`
     * is assigned randomly per process when the node is created.
     */
    private[this] def tokenOrder(vec: Vector[Node]): Vector[Node] =
      vec.sortBy(nodeToken)

    /**
     * Returns a new vector with the nodes ordered relative to the coordinate in
     * `coord`. This gives the distributor a deterministic order across process
     * boundaries.
     */
    private[this] def ringOrder(vec: Vector[Node], coord: Double): Vector[Node] = {
      val order = new Ring(vec.size).alternatingIter(coord)
      val builder = new VectorBuilder[Node]
      while (order.hasNext) { builder += vec(order.next()) }
      builder.result
    }

    /**
     * Returns a new vector which is ordered by the node's status. Note, it is
     * important that this is a stable sort since we care about the source
     * order when using deterministic ordering.
     */
    private[this] def statusOrder(
      vec: Vector[Node],
      busyBuilder: VectorBuilder[Node]
    ): Vector[Node] = {
      val resultNodes = new VectorBuilder[Node]
      val busyNodes = new ListBuffer[Node]
      val closedNodes = new ListBuffer[Node]

      val iter = vec.iterator
      while (iter.hasNext) {
        val node = iter.next()
        node.status match {
          case Status.Open   => resultNodes += node
          case Status.Busy   => busyNodes += node
          case Status.Closed => closedNodes += node
        }
      }

      busyBuilder ++= busyNodes
      resultNodes ++= busyNodes ++= closedNodes
      resultNodes.result
    }

    /**
     * Rebuilds the distributor and sorts the vector in two possible ways:
     *
     * 1. If `useDeterministicOrdering` is set to true and [[DeterministicOrdering]]
     * has a coordinate set, then the coordinate is used which gives the
     * distributor a well-defined, deterministic order across process boundaries.
     *
     * 2. Otherwise, the vector is sorted by a node's token field.
     */
    def rebuild(vec: Vector[Node]): This = {
     if (vec.isEmpty) {
        new Distributor(vec, vec, busy, coordinate, aperture)
      } else {
        DeterministicOrdering() match {
          case someCoord@Some(coord) if useDeterministicOrdering =>
            val busyBuilder = new VectorBuilder[Node]
            val newVec = statusOrder(ringOrder(vec, coord.value), busyBuilder)
            new Distributor(newVec, vec, busyBuilder.result, someCoord, aperture)
          case _ =>
            val busyBuilder = new VectorBuilder[Node]
            val newVec = statusOrder(tokenOrder(vec), busyBuilder)
            new Distributor(newVec, vec, busyBuilder.result, None, aperture)
        }
      }
    }

    // To reduce the amount of rebuilds needed, we rely on the probabilistic
    // nature of the p2c pick. That is, we know that only when a significant
    // portion of the underlying vector is unavailable will we return an
    // unavailable node to the layer above and trigger a rebuild. We do however
    // want to return to our "stable" ordering as soon as we notice that a
    // previously busy node is now available.
    def needsRebuild: Boolean = busy.exists(openNode)
  }

  protected def initDistributor(): Distributor =
    new Distributor(Vector.empty, Vector.empty, Vector.empty, None, 1)

  override def close(deadline: Time): Future[Unit] = {
    gauges.foreach(_.remove())
    coordObservation.close(deadline).before { super.close(deadline) }
  }
}