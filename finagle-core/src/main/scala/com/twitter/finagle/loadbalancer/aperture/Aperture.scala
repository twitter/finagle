package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Address.Inet
import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.Balancer
import com.twitter.finagle.util.Rng
import com.twitter.logging.Logger
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import scala.util.hashing.MurmurHash3

private object Aperture {

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

  type Node <: ApertureNode[Req, Rep]

  /**
   * The random number generator used to pick two nodes for
   * comparison – since aperture uses p2c for selection.
   */
  private[aperture] def rng: Rng

  /**
   * The minimum aperture as specified by the user config. Note this value is advisory
   * and the distributor may actually derive a new min based on this.  See `minUnits`
   * for more details.
   */
  private[aperture] def minAperture: Int

  /**
   * Enables [[Aperture]] to create weight-aware balancers
   */
  private[aperture] def manageWeights: Boolean

  /**
   * Overrides [[minAperture]] when >=1.
   */
  private[aperture] def minApertureOverride: Int

  /**
   * Enables [[Aperture]] to read coordinate data from [[ProcessCoordinate]]
   * to derive an ordering for the endpoints used by this [[Balancer]] instance.
   */
  protected val useDeterministicOrdering: Option[Boolean]

  /**
   * Indicator if the endpoints within the aperture should be connected to eagerly. This is a Function0
   * to allow the capability to switch off the feature without having to reconstruct the client stack.
   */
  private[aperture] def eagerConnections: Boolean

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

  private[aperture] def dapertureActive: Boolean = {
    useDeterministicOrdering match {
      case Some(bool) => bool
      case None => ProcessCoordinate().isDefined
    }
  }

  @volatile private[this] var _vectorHash: Int = -1

  // Make a hash of the passed in `vec` and set `vectorHash`.
  private[aperture] def updateVectorHash(vec: Vector[Node]): Unit = {
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

  private[aperture] def lbl = if (label.isEmpty) "<unlabelled>" else label
  // `pickLog` will log on the hot path so should be enabled judiciously.
  private[aperture] val pickLog =
    Logger.get(s"com.twitter.finagle.loadbalancer.aperture.Aperture.pick-log.$lbl")
  // `rebuildLog` is used for rebuild level events which happen at a relatively low frequency.
  private[aperture] val rebuildLog =
    Logger.get(s"com.twitter.finagle.loadbalancer.aperture.Aperture.rebuild-log.$lbl")

  protected type Distributor = BaseDist[Req, Rep, Node]

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

  private[aperture] def mkDeterministicAperture(
    vector: Vector[Node],
    initAperture: Int,
    coord: Coord
  ): BaseDist[Req, Rep, Node] = {
    if (manageWeights) {
      new WeightedAperture[Req, Rep, Node](
        this,
        vector,
        initAperture,
        coord
      )
    } else {
      new DeterministicAperture[Req, Rep, Node](
        this,
        vector,
        initAperture,
        coord
      )
    }
  }

  private[aperture] def mkRandomAperture(
    vector: Vector[Node],
    initAperture: Int
  ): BaseDist[Req, Rep, Node] = {
    new RandomAperture[Req, Rep, Node](
      this,
      vector = vector,
      initAperture = initAperture
    )
  }

  private[aperture] def mkEmptyVector(size: Int) =
    new EmptyVector[Req, Rep, Node](this, size)

  protected def initDistributor(): Distributor = new EmptyVector[Req, Rep, Node](
    this,
    initAperture = 1
  )

  override def close(deadline: Time): Future[Unit] = {
    gauges.foreach(_.remove())
    // If manageWeights is true, the Balancer is not wrapped in a TrafficDistributor and therefore
    // needs to manage its own lifecycle.
    val closeNodes = if (!manageWeights) Closable.nop else Closable.all(dist.vector: _*)
    coordObservation
      .close(deadline)
      .before { closeNodes.close(deadline) }
      .before { super.close(deadline) }
  }

  override def status: Status = dist.status
}
