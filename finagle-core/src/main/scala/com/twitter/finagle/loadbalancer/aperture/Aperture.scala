package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.Address.Inet
import com.twitter.finagle.loadbalancer.p2c.P2CPick
import com.twitter.finagle.loadbalancer.{Balancer, DistributorT, NodeT}
import com.twitter.finagle.util.Rng
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future, Time}
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3

private object Aperture {
  private[this] val log = Logger.get()

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
  private[aperture] val MinDeterministicAperture: Int = {
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

  /**
   * Compute the width of the aperture slice using the logical aperture size and the local
   * and remote ring unit widths.
   */
  def dApertureWidth(
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

  // Only an Inet address of the factory is considered and all
  // other address types are ignored.
  private[aperture] def computeVectorHash(it: Iterator[Address]): Int = {
    // A specialized reimplementation of MurmurHash3.listHash
    var n = 0
    var h = MurmurHash3.arraySeed
    while (it.hasNext) it.next() match {
      case Inet(addr, _) if !addr.isUnresolved =>
        val d = MurmurHash3.bytesHash(addr.getAddress.getAddress)
        h = MurmurHash3.mix(h, d)
        n += 1

      case _ => // no-op
    }

    MurmurHash3.finalizeHash(h, n)
  }
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
   * Indicator if the endpoints within the aperture should be connected to eagerly. This is a Function0
   * to allow the capability to switch off the feature without having to reconstruct the client stack.
   */
  protected def eagerConnections: Boolean

  /**
   * Adjust the aperture by `n` serving units.
   *
   * Calls to this method are intrinsically racy with respect to updates and rebuilds
   * and no special consideration is taken to avoid these races as feedback mechanisms
   * should simply fire again should an adjustment be made to an old [[Balancer]].
   */
  protected def adjust(n: Int): Unit = dist.adjust(n)

  /**
   * Widen the aperture by one serving unit.
   */
  protected def widen(): Unit = adjust(1)

  /**
   * Narrow the aperture by one serving unit.
   */
  protected def narrow(): Unit = adjust(-1)

  /**
   * The current logical aperture. This is never less than 1, or more
   * than `maxUnits`.
   */
  protected def logicalAperture: Int = dist.logicalAperture

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

  protected def dapertureActive: Boolean = {
    if (ProcessCoordinate().isEmpty) {
      false
    } else {
      useDeterministicOrdering match {
        case Some(bool) => bool
        case None => true
      }
    }
  }

  @volatile private[this] var _vectorHash: Int = -1

  // Make a hash of the passed in `vec` and set `vectorHash`.
  private[this] def updateVectorHash(vec: Vector[Node]): Unit = {
    val addrs = vec.iterator.map(_.factory.address)
    _vectorHash = Aperture.computeVectorHash(addrs)
  }

  protected[this] def vectorHash: Int = _vectorHash

  private[this] val gauges = Seq(
    statsReceiver.addGauge("logical_aperture") { logicalAperture },
    statsReceiver.addGauge("physical_aperture") { dist.physicalAperture },
    statsReceiver.addGauge("use_deterministic_ordering") {
      if (dapertureActive) 1f else 0f
    },
    statsReceiver.addGauge("eager_connections") {
      if (eagerConnections) 1f else 0f
    },
    statsReceiver.addGauge("vector_hash") { _vectorHash }
  )

  private[this] val coordinateUpdates = statsReceiver.counter("coordinate_updates")

  private[this] val coordObservation = ProcessCoordinate.changes.respond { _ =>
    // One nice side-effect of deferring to the balancers `updater` is
    // that we serialize and collapse concurrent updates. So if we have a volatile
    // source that is updating the coord, we are resilient to that. We could
    // go even further by rate limiting the changes if we need to.
    coordinateUpdates.incr()
    self.rebuild()
  }

  private[this] def lbl = if (label.isEmpty) "<unlabelled>" else label
  // `pickLog` will log on the hot path so should be enabled judiciously.
  private val pickLog =
    Logger.get(s"com.twitter.finagle.loadbalancer.aperture.Aperture.pick-log.$lbl")
  // `rebuildLog` is used for rebuild level events which happen at a relatively low frequency.
  private val rebuildLog =
    Logger.get(s"com.twitter.finagle.loadbalancer.aperture.Aperture.rebuild-log.$lbl")

  protected type Distributor = BaseDist

  def additionalMetadata: Map[String, Any] = {
    Map(
      "distributor_class" -> dist.getClass.getSimpleName,
      "logical_aperture_size" -> dist.logicalAperture,
      "physical_aperture_size" -> dist.physicalAperture,
      "min_aperture_size" -> dist.min,
      "max_aperture_size" -> dist.max,
      "vector_hash" -> vectorHash
    ) ++ dist.additionalMetadata
  }

  /**
   * A distributor which implements the logic for controlling the size of an aperture
   * but defers the implementation of pick to concrete implementations.
   *
   * @param vector The source vector received from a call to `rebuild`.
   *
   * @param initAperture The initial aperture to use.
   */
  protected abstract class BaseDist(vector: Vector[Node], initAperture: Int)
      extends DistributorT[Node](vector) {
    type This = BaseDist

    /**
     * Returns the maximum size of the aperture window.
     */
    final def max: Int = vector.size

    /**
     * Returns the minimum size of the aperture window.
     */
    def min: Int = math.min(minAperture, vector.size)

    // We are guaranteed that writes to aperture are serialized since
    // we only expose them via the `narrow`, `widen`, etc. methods above. Those
    // defer to the balancers `updater` which is serial. Therefore, we only
    // need to guarantee visibility across threads and don't need to
    // provide other synchronization between threads.
    @volatile private[this] var _logicalAperture: Int = initAperture
    // Make sure the aperture is within bounds [min, max].
    adjust(0)

    /**
     * Returns the current logical aperture.
     */
    def logicalAperture: Int = _logicalAperture

    /**
     * Represents how many servers `pick` will select over – which may
     * differ from `logicalAperture` when using [[DeterministicAperture]].
     */
    def physicalAperture: Int = logicalAperture

    /**
     * Adjusts the logical aperture by `n` while ensuring that it stays
     * within the bounds [min, max].
     */
    final def adjust(n: Int): Unit = {
      _logicalAperture = math.max(min, math.min(max, _logicalAperture + n))
    }

    /**
     * A flag indicating that this distributor has been discarded due to a rebuild.
     */
    @volatile private[this] var rebuilt: Boolean = false

    final def rebuild(): This = rebuild(vector)

    def rebuild(vec: Vector[Node]): This = {
      rebuilt = true

      updateVectorHash(vec)
      val dist = if (vec.isEmpty) {
        new EmptyVector(initAperture)
      } else if (dapertureActive) {
        ProcessCoordinate() match {
          case Some(coord) =>
            new DeterministicAperture(vec, initAperture, coord)
          case None =>
            // this should not happen as `dapertureActive` should prevent this case
            // but hypothetically, the coordinate could get unset between calls
            // to `dapertureActive` and `ProcessCoordinate()`
            new RandomAperture(vec, initAperture)
        }
      } else {
        new RandomAperture(vec, initAperture)
      }

      if (self.eagerConnections) {
        val oldNodes = indices.map(vector(_))
        dist.doEagerlyConnect(oldNodes)
      }

      dist
    }

    /**
     * Eagerly connects to the new endpoints within the aperture. The connections created are
     * out of band without any timeouts. If an in-flight request picks a host that has no
     * established sessions, a request-driven connection will be established.
     */
    private def doEagerlyConnect(oldNodes: Set[Node]): Unit = {
      val is = indices
      if (rebuildLog.isLoggable(Level.DEBUG)) {
        val newEndpoints = is.count(i => !oldNodes.contains(vector(i)))
        rebuildLog.debug(s"establishing $newEndpoints eager connections")
      }

      is.foreach { i =>
        val node = vector(i)
        if (!oldNodes.contains(node)) {
          ApertureEagerConnections.submit {
            if (rebuilt) Future.Done
            else node().flatMap(svc => svc.close())
          }
        }
      }
    }

    /**
     * Returns the indices which are currently part of the aperture. That is,
     * the indices over which `pick` selects.
     */
    def indices: Set[Int]

    /*
     * Returns the best status of nodes within `indices`
     */
    def status: Status

    def additionalMetadata: Map[String, Any]
  }

  /**
   * A distributor which has an aperture size but an empty vector to select
   * from, so it always returns the `failingNode`.
   */
  protected class EmptyVector(initAperture: Int) extends BaseDist(Vector.empty, initAperture) {
    require(vector.isEmpty, s"vector must be empty: $vector")
    def indices: Set[Int] = Set.empty
    def status: Status = Status.Closed
    def pick(): Node = failingNode(emptyException)
    def needsRebuild: Boolean = false
    def additionalMetadata: Map[String, Any] = Map.empty
  }

  // these are lifted out of `RandomAperture` to avoid unnecessary allocations.
  private[this] val nodeToken: ApertureNode => Int = _.token
  private[this] val nodeOpen: ApertureNode => Boolean = _.status == Status.Open
  private[this] val nodeBusy: ApertureNode => Boolean = _.status == Status.Busy

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
  protected final class RandomAperture(vector: Vector[Node], initAperture: Int)
      extends BaseDist(vector, initAperture)
      with P2CPick[Node] {
    require(vector.nonEmpty, "vector must be non empty")

    /**
     * Returns a new vector which is ordered by a node's status. Note, it is
     * important that this is a stable sort since we care about the source order
     * of `vec` to eliminate any unnecessary resource churn.
     */
    private[this] def statusOrder(vec: Vector[Node]): Vector[Node] = {
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

    // Since we don't have any process coordinate, we sort the node
    // by `token` which is deterministic across rebuilds but random
    // globally, since `token` is assigned randomly per process
    // when the node is created.
    protected val vec: Vector[Node] = statusOrder(vector.sortBy(nodeToken))
    protected def bound: Int = logicalAperture
    protected def emptyNode: Node = failingNode(emptyException)
    protected def rng: Rng = self.rng

    private[this] def vecAsString: String =
      vec
        .take(logicalAperture)
        .map(_.factory.address)
        .mkString("[", ", ", "]")

    if (rebuildLog.isLoggable(Level.DEBUG)) {
      rebuildLog.debug(s"[RandomAperture.rebuild $lbl] nodes=$vecAsString")
    }

    def indices: Set[Int] = (0 until logicalAperture).toSet

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
  protected final class DeterministicAperture(vector: Vector[Node], initAperture: Int, coord: Coord)
      extends BaseDist(vector, initAperture) {
    require(vector.nonEmpty, "vector must be non empty")

    private[this] val ring = new Ring(vector.size, rng)

    // Note that this definition ignores the user defined `minAperture`,
    // but that isn't likely to hold much value given our definition of `min`
    // and how we calculate the `apertureWidth`.
    override def min: Int = math.min(Aperture.MinDeterministicAperture, vector.size)

    // DeterministicAperture does not dynamically adjust the aperture based on load
    override def logicalAperture: Int = min

    // Translates the logical `aperture` into a physical one that
    // maps to the ring. Note, we do this in terms of the peer
    // unit width in order to ensure full ring coverage. As such,
    // this width will be >= the aperture. Put differently, we may
    // cover more servers than the `aperture` requested in service
    // of global uniform load.
    private[this] def apertureWidth: Double =
      Aperture.dApertureWidth(coord.unitWidth, ring.unitWidth, logicalAperture)

    override def physicalAperture: Int = {
      val width = apertureWidth
      if (rebuildLog.isLoggable(Level.DEBUG)) {
        rebuildLog.debug(
          f"[DeterministicAperture.physicalAperture $lbl] ringUnit=${ring.unitWidth}%1.6f coordUnit=${coord.unitWidth}%1.6f coordOffset=${coord.offset}%1.6f apertureWidth=$width%1.6f"
        )
      }
      ring.range(coord.offset, width)
    }

    override val indices: Set[Int] = ring.indices(coord.offset, apertureWidth).toSet

    private[this] def nodes: Set[(Int, Double, Address, Status)] = {
      indices.map { i =>
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
    if (rebuildLog.isLoggable(Level.DEBUG)) {
      val apertureSlice: String = {
        nodes
          .map {
            case (i, weight, addr, status) =>
              f"(index=$i, weight=$weight%1.6f, addr=$addr, status=$status)"
          }.mkString("[", ", ", "]")
      }
      rebuildLog.debug(s"[DeterministicAperture.rebuild $lbl] nodes=$apertureSlice")

      // It may be useful see the raw server vector for d-aperture since we expect
      // uniformity across processes.
      if (rebuildLog.isLoggable(Level.TRACE)) {
        val vectorString = vector.map(_.factory.address).mkString("[", ", ", "]")
        rebuildLog.trace(s"[DeterministicAperture.rebuild $lbl] nodes=$vectorString")
      }
    }

    // A quick helper for seeing if a is close to the same value as b.
    // This allows for avoiding bias due to numerical instability in
    // floating point values.
    private[this] def approxEqual(a: Double, b: Double): Boolean =
      math.abs(a - b) < 0.0001

    /**
     * Pick the least loaded (and healthiest) of the two nodes `a` and `b`
     * taking into account their respective weights.
     */
    private[this] def pick(a: Node, aw: Double, b: Node, bw: Double): Node = {
      val aStatus = a.status
      val bStatus = b.status
      if (aStatus == bStatus) {
        // Note, `aw` or `bw` can't be zero since `pick2` would not
        // have returned the indices in the first place. However,
        // we check anyways to safeguard against any numerical
        // stability issues.
        val _aw = if (aw == 0) 1.0 else aw
        val _bw = if (bw == 0) 1.0 else bw

        // We first check if the weights are effectively the same so we can
        // ignore them if they are. If we just go for it and use them we can
        // evaluate (1.0 / 1.0 <= 1.0 / 1.0000000001) and we bias toward the
        // second instance which should have identical weight but it's a hair
        // off due to floating point precision errors.
        if (approxEqual(_aw, _bw)) {
          if (a.load <= b.load) a else b
        } else {
          if (a.load / _aw <= b.load / _bw) a else b
        }
      } else {
        if (Status.best(aStatus, bStatus) == aStatus) a else b
      }
    }

    def pick(): Node = {
      val offset = coord.offset
      val width = apertureWidth
      val a = ring.pick(offset, width)
      val b = ring.tryPickSecond(a, offset, width)
      val aw = ring.weight(a, offset, width)
      val bw = ring.weight(b, offset, width)

      val nodeA = vector(a)
      val nodeB = vector(b)
      val picked = pick(nodeA, aw, nodeB, bw)

      if (pickLog.isLoggable(Level.TRACE)) {
        pickLog.trace(
          f"[DeterministicAperture.pick] a=(index=$a, weight=$aw%1.6f, node=$nodeA) b=(index=$b, weight=$bw%1.6f, node=$nodeB) picked=$picked"
        )
      }

      picked
    }

    // rebuilds only need to happen when we receive ring updates (from
    // the servers or our coordinate changing).
    def needsRebuild: Boolean = false

    // Although `needsRebuild` is set to false, the Balancer will trigger a rebuild
    // when it exhausts picking busy nodes. Lets explictly return the same distributor
    // if the coordinates and server set hasn't changed.
    override def rebuild(newVector: Vector[Node]): This =
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

  protected def initDistributor(): Distributor = new EmptyVector(1)

  override def close(deadline: Time): Future[Unit] = {
    gauges.foreach(_.remove())
    coordObservation.close(deadline).before { super.close(deadline) }
  }

  override def status: Status = dist.status
}
