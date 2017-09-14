package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.CoreToggles
import com.twitter.finagle.loadbalancer.{Balancer, NodeT, DistributorT}
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.util.Rng
import com.twitter.logging.Logger
import com.twitter.util.{Future, Time}
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer

private object Aperture {
  private val log = Logger.get()

  val dapertureToggleKey = "com.twitter.finagle.core.UseDeterministicAperture"
  private val dapertureToggle = CoreToggles(dapertureToggleKey)
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
  import ProcessCoordinate._
  import Aperture._

  protected type Node <: ApertureNode

  protected trait ApertureNode extends NodeT[Req, Rep] {

    /**
     * A token is a random integer associated with an Aperture node.
     * It persists through node updates, but is not necessarily
     * unique. Aperture uses this token to order the nodes when
     * deterministic ordering is not enabled or available. Since
     * the token is assigned at Node creation, this guarantees
     * a stable order across distributor rebuilds.
     */
    val token: Int = rng.nextInt()
  }

  /**
   * The random number generator used to pick two nodes for
   * comparison – since aperture uses p2c for selection.
   */
  protected def rng: Rng

  /**
   * The minimum aperture as specified by the user config. Note this value is advisory
   * and the distributor may actually derive a new min based on this.  See `minUnits`
   * for more details.
   */
  protected def minAperture: Int

  /**
   * Enables [[Aperture]] to read coordinate data from [[ProcessCoordinate]]
   * to derive an ordering for the endpoints used by this [[Balancer]] instance.
   */
  protected def useDeterministicOrdering: Option[Boolean]

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
   * The maximum aperture serving units.
   */
  protected def maxUnits: Int = dist.max

  /**
   * The minimum aperture serving units.
   */
  protected def minUnits: Int = dist.min

  /**
   * Label used to identify this instance when logging internal state.
   */
  protected def label: String

  // We set the toggle to take into account both the cluster id and the `label`
  // for the client. Effectively, we want all the clients of a particular
  // cluster to be included during the same experiment window since d-aperture
  // needs all the respective clients to participate in order to be effective.
  private[this] def dapertureToggleActive: Boolean =
    dapertureToggle(s"${ServerInfo().clusterId}:$label".hashCode)

  private[this] def dapertureActive: Boolean =
    useDeterministicOrdering match {
      case Some(bool) => bool
      case None => dapertureToggleActive
    }

  private[this] val gauges = Seq(
    statsReceiver.addGauge("aperture") { aperture },
    statsReceiver.addGauge("physical_aperture") { dist.physicalAperture },
    statsReceiver.addGauge("use_deterministic_ordering") {
      if (dapertureActive) 1F else 0F
    }
  )

  private[this] val coordinateUpdates = statsReceiver.counter("coordinate_updates")
  // note, this can be lifted to a "verbose/debug" counter when we are sufficiently
  // confident in d-aperture.
  private[this] val noCoordinate = statsReceiver.counter("rebuild_no_coordinate")

  private[this] val coordObservation = ProcessCoordinate.changes.respond { _ =>
    // One nice side-effect of deferring to the balancers `updater` is
    // that we serialize and collapse concurrent updates. So if we have a volatile
    // source that is updating the coord, we are resilient to that. We could
    // go even further by rate limiting the changes if we need to.
    coordinateUpdates.incr()
    self.rebuild()
  }

  private[this] val nodeToken: ApertureNode => Int = _.token
  private[this] val nodeOpen: ApertureNode => Boolean = _.status == Status.Open
  private[this] val nodeBusy: ApertureNode => Boolean = _.status == Status.Busy

  protected type Distributor = BaseDist

  /**
   * A distributor which implements the logic for controlling the size of an aperture
   * but defers the implementation of pick to concrete implementations.
   *
   * @param vector The source vector received from a call to `rebuild`.
   *
   * @param initAperture The initial aperture to use.
   */
  protected abstract class BaseDist(
    vector: Vector[Node],
    initAperture: Int
  ) extends DistributorT[Node](vector) {
    type This = BaseDist

    /*
     * Returns a new vector which is ordered by the node's status. Note, it is
     * important that this is a stable sort since we care about the source order
     * of `vec`.
     */
    protected def statusOrder(vec: Vector[Node]): Vector[Node] = {
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

    /**
     * Returns the maximum size of the aperture window.
     */
    def max: Int = vector.size

    /**
     * Returns the minimum size of the aperture window.
     */
    def min: Int = math.min(minAperture, vector.size)

    // We are guaranteed that writes to aperture are serialized since
    // we only expose them via the `narrow`, `widen`, etc. methods above. Those
    // defer to the balancers `updater` which is serial. Therefore, we only
    // need to guarantee visibility across threads and don't need to
    // provide other synchronization between threads.
    @volatile private[this] var _aperture: Int = initAperture
    // Make sure the aperture is within bounds [min, max].
    adjust(0)

    /**
     * Returns the current aperture.
     */
    def aperture: Int = _aperture

    /**
     * Represents how many servers `pick` will select over – which may
     * differ from `aperture` when using [[DeterministicAperture]].
     */
    def physicalAperture: Int = aperture

    /**
     * Adjusts the aperture by `n` while ensuring that it stays within
     * the bounds [min, max].
     */
    def adjust(n: Int): Unit = {
      _aperture = math.max(min, math.min(max, _aperture + n))
    }

    def rebuild(): This = rebuild(vector)
    def rebuild(vec: Vector[Node]): This =
      if (vec.isEmpty) new EmptyVector(initAperture)
      else ProcessCoordinate() match {
        case Some(coord) if dapertureActive =>
          new DeterministicApeture(vec, initAperture, coord)

        case None if dapertureActive =>
          noCoordinate.incr()
          new RandomAperture(vec, initAperture)

        case _ =>
          new RandomAperture(vec, initAperture)
      }

    /**
     * Pick the least loaded (and healthiest) of the two nodes `a` and `b`
     * taking into account their respective weights.
     */
    protected def pick(a: Node, aw: Double, b: Node, bw: Double): Node = {
      if (a.status == b.status) {
        if (a.load / aw <= b.load / bw) a else b
      } else {
        if (Status.best(a.status, b.status) == a.status) a else b
      }
    }

    /**
     * Returns the indices which are currently part of the aperture. That is,
     * the indices over which `pick` selects.
     */
    def indices: Set[Int]

    /**
     * Returns the least loaded node within the aperture window.
     */
    def pick(): Node

    // To reduce the amount of rebuilds needed, we rely on the probabilistic
    // nature of p2c pick. That is, we know that only when a significant
    // portion of the underlying vector is unavailable will we return an
    // unavailable node to the layer above and trigger a rebuild. We do however
    // want to return to our "stable" ordering as soon as we notice that a
    // previously busy node is now available.
    private[this] val busy = vector.filter(nodeBusy)
    def needsRebuild: Boolean = busy.exists(nodeOpen)
  }

  /**
   * A distributor which has an aperture size but an empty vector to select
   * from, so it always returns the `failingNode`.
   */
  protected class EmptyVector(initAperture: Int)
    extends BaseDist(Vector.empty, initAperture) {
      require(vector.isEmpty, s"vector must be empty: $vector")
      def indices: Set[Int] = Set.empty
      def pick(): Node = failingNode(emptyException)
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
   *
   * @param initAperture The initial aperture to use.
   */
  protected class RandomAperture(
    vector: Vector[Node],
    initAperture: Int
  ) extends BaseDist(vector, initAperture) {
    require(vector.nonEmpty, "vector must be non empty")

    // Since we don't have any process coordinate, we sort the node
    // by `token` which is deterministic across rebuilds but random
    // globally, since `token` is assigned randomly per process
    // when the node is created.
    private[this] val vec = statusOrder(vector.sortBy(nodeToken))

    def indices: Set[Int] = (0 until aperture).toSet

    def pick(): Node = {
      if (aperture <= 1) vec.head
      else {
        val a = rng.nextInt(aperture)
        var b = rng.nextInt(aperture - 1)
        if (b >= a) { b += 1 }
        pick(vec(a), 1.0, vec(b), 1.0)
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
   *
   * @param initAperture The initial aperture to use.
   *
   * @param coord The [[ProcessCoordinate]] for this process which is used to narrow
   * the range of `pick2`.
   */
  protected class DeterministicApeture(
    vector: Vector[Node],
    initAperture: Int,
    coord: Coord
  ) extends BaseDist(vector, initAperture) {
    require(vector.nonEmpty, "vector must be non empty")

    private[this] val ring = new Ring(vector.size, rng)

    // We log the contents of the aperture on each distributor rebuild when using
    // deterministic aperture. Rebuilds are not frequent and concentrated around
    // events where this information would be valuable (i.e. coordinate changes or
    // host add/removes).
    {
      val apertureSlice: String = {
        val offset = coord.offset
        val width = apertureWidth
        val indices = ring.indices(offset, width)
        indices.map { i =>
          val addr = vector(i).factory.address
          val weight = ring.weight(i, offset, width)
          f"(index=$i, weight=$weight%1.3f, addr=$addr)"
        }.mkString("[", ", ", "]")
      }
      val lbl = if (label.isEmpty) "<unlabelled>" else label
      log.debug(s"Aperture updated for client $lbl: nodes=$apertureSlice")
    }

    // We want to additionally ensure that p2c can actually converge when there
    // are weights present. In order to do so, we need to pick over 4 or more servers.
    // To see why, we can think about this in terms of picking one weighted node.
    // If we only have one node to choose from, it's impossible to respect the weight
    // since we will always return the single node – we need at least 2 nodes in
    // this case. The same holds true for pick2, except we need 4 so that the
    // weight(s) hold. Additionally, a min value of 4 nodes is more practical
    // for resiliency. Note that this definition ignores the user defined `minAperture`,
    // but that isn't likely to hold much value given our definition of `min`
    // and how we calculate the `apertureWidth`.
    override def min: Int = math.min(4, vector.size)

    // Translates the logical `aperture` into a physical one that
    // maps to the ring. Note, we do this in terms of the peer
    // unit width in order to ensure full ring coverage. As such,
    // this width will be >= the aperture. Put differently, we may
    // cover more servers than the `aperture` requested in service
    // of global uniform load.
    private[this] def apertureWidth: Double = {
      val unitWidth: Double = coord.unitWidth // (0, 1.0]
      val unitAperture: Double  = aperture * ring.unitWidth // (0, 1.0]
      val units: Int = math.ceil(unitAperture / unitWidth).toInt
      val width: Double = units * unitWidth
      // We know that `width` is bounded between (0, 1.0] since `units`
      // at most will be the inverse of `unitWidth` (i.e. if `unitAperture`
      // is 1, then units = 1/(1/x) = x, width = x*(1/x) = 1). However,
      // practically, we take the min of 1.0 to account for any floating
      // point stability issues.
      math.min(1.0, width)
    }

    override def physicalAperture: Int = ring.range(coord.offset, apertureWidth)

    // We only need to order by status since the ring does the rest w.r.t
    // to picking within the processes coordinate. Also, note, that the
    // order of `vector` is stable across processes since it is sorted
    // by [[LoadBalancerFactory]].
    private[this] val vec = statusOrder(vector)

    def indices: Set[Int] = ring.indices(coord.offset, apertureWidth).toSet

    def pick(): Node = {
      val offset = coord.offset
      val width = apertureWidth
      val a = ring.pick(offset, width)
      val b = ring.tryPickSecond(a, offset, width)
      val aw = ring.weight(a, offset, width)
      val bw = ring.weight(b, offset, width)
      // Note, `aw` or `bw` can't be zero since `pick2` would not
      // have returned the indices in the first place.
      pick(vec(a), aw, vec(b), bw)
    }
  }

  protected def initDistributor(): Distributor = new EmptyVector(1)

  override def close(deadline: Time): Future[Unit] = {
    gauges.foreach(_.remove())
    coordObservation.close(deadline).before { super.close(deadline) }
  }
}
