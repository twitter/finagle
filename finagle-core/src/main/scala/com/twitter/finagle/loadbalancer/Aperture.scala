package com.twitter.finagle.loadbalancer

import com.twitter.conversions.time._
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.{Rng, Ring, Ema, DefaultTimer}
import com.twitter.finagle.{
  ClientConnection, NoBrokersAvailableException, ServiceFactory, ServiceFactoryProxy, 
  ServiceProxy, Status}
import com.twitter.util.{Activity, Return, Future, Throw, Time, Var, Duration, Timer}
import java.util.concurrent.atomic.AtomicInteger

object ApertureBalancerFactory extends WeightedLoadBalancerFactory {
  def newLoadBalancer[Req, Rep](
    factories: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] =
    newWeightedLoadBalancer(
      Activity(factories map(Activity.Ok(_))), 
      statsReceiver, emptyException)

  def newWeightedLoadBalancer[Req, Rep](
    activity: Activity[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] =
    new ApertureLoadBandBalancer(activity,
      statsReceiver = statsReceiver,
      emptyException = emptyException)
}



/**
 * The aperture load-band balancer balances load to the smallest
 * subset ("aperture") of services so that:
 *
 *  1. The concurrent load, measured over a window specified by
 *     `smoothWin`, to each service stays within the load band, delimited
 *     by `lowLoad` and `highLoad`.
 *  1. Services receive load proportional to the ratio of their
 *     weights.
 *
 * Unavailable services are not counted--the aperture expands as
 * needed to cover those that are available.
 *
 * For example, if the load band is [0.5, 2], the aperture will be
 * adjusted so that no service inside the aperture has a load less
 * than 0.5 or more then 2, so long as offered load permits it.
 *
 * The default load band, [0.5, 2], matches closely the load distribution
 * given by least-loaded without any aperturing.
 *
 * Among the benefits of aperture balancing are:
 *
 *  1. A client uses resources commensurate to offered load. In particular,
 *     it does not have to open sessions with every service in a large cluster.
 *     This is especially important when offered load and cluster capacity 
 *     are mismatched.
 *  1. It balances over fewer, and thus warmer, services. This enhances the 
 *     efficacy of the fail-fast mechanisms, etc. This also means that clients pay
 *     the penalty of session establishment less frequently.
 *  1. It increases the efficacy of least-loaded balancing which, in order to 
 *     work well, requires concurrent load. The load-band balancer effectively
 *     arranges load in a manner that ensures a higher level of per-service
 *     concurrency.
 */
private class ApertureLoadBandBalancer[Req, Rep](
    protected val activity: Activity[Traversable[(ServiceFactory[Req, Rep], Double)]],
    protected val smoothWin: Duration = 5.seconds,
    protected val lowLoad: Double = 0.5,
    protected val highLoad: Double = 2,
    protected val maxEffort: Int = 5,
    protected val rng: Rng = Rng.threadLocal,
    protected implicit val timer: Timer = DefaultTimer.twitter,
    protected val statsReceiver: StatsReceiver = NullStatsReceiver,
    protected val emptyException: NoBrokersAvailableException = new NoBrokersAvailableException)
  extends Balancer[Req, Rep]
  with Aperture[Req, Rep]
  with LoadBand[Req, Rep]
  with Updating[Req, Rep]

object Aperture {
  private val W = Int.MaxValue
}

/**
 * The aperture distributor balances load onto a window--the
 * aperture--of underlying capacity. The distributor exposes a
 * control mechanism so that a controller can adjust the aperture
 * according to load conditions.
 *
 * The window contains a number of discrete serving units, each of
 * which corresponds to the serving capacity represented by a unit
 * weight. Thus a single serving unit may be half of a node with
 * weight=2, or two nodes with weight=0.5. This arrangement allows
 * the aperture distributor to maintain the weight contract: in
 * aggregate, endpoints are assigned loads according to their weight
 * and the current load metric. No load metric is prescribed: this
 * can be mixed in separately.
 *
 * The underlying nodes are arranged in a consistent fashion: an
 * aperture of a given size always refers to the same set of nodes; a
 * smaller aperture to a subset of those nodes. Thus it is relatively
 * harmless to adjust apertures frequently, since underlying nodes
 * are typically backed by pools, and will be warm on average.
 */
private trait Aperture[Req, Rep] { self: Balancer[Req, Rep] =>
  import Aperture._

  protected def rng: Rng

  private[this] val nodeUp: Node => Boolean = 
    { node => node.status == Status.Open && node.weight > 0 }

  private[this] val gauge = statsReceiver.addGauge("aperture") { aperture }

  protected class Distributor(val vector: Vector[Node], initAperture: Int)
    extends DistributorT {
    type This = Distributor

    @volatile private[this] var sawDown = false

    private[this] val (up, down) = vector.partition(nodeUp) match {
      case (Vector(), down) => (down, Vector.empty)
      case updown => updown
    }

    private[this] val (ring, width, max) = 
      if (up.isEmpty) {
        (Ring.fromWeights(Seq(1), W), W, W)
      } else {
        val N = up.size
        val weights = up.map(_.weight)
        val sum = weights.sum
        val ring =
          if (sum == 0)
            Ring.fromWeights(Seq.fill(N)(1), W)
          else
            Ring.fromWeights(weights, W)
        val width = (W/sum).toInt
        (ring, width, W/width)
      }

    @volatile private[Aperture] var aperture = initAperture
    // Make sure the aperture is within bounds [1, max].
    adjust(0)

    protected[Aperture] def adjust(n: Int) {
      aperture = math.max(1, math.min(max, aperture+n))
    }

    def rebuild() = 
      new Distributor(vector, aperture)

    def rebuild(vector: Vector[Node]) = 
      new Distributor(vector.sortBy(_.token), aperture)

    /**
     * The number of available serving units.
     */
    def units = max
    
    // We use power of two choices to pick nodes. This keeps things
    // simple, but we could reasonably use a heap here, too.
    def pick(): Node = {
      if (up.isEmpty)
        return failingNode(emptyException)

      if (up.size == 1)
        return up(0)

      // TODO(marius): return fractional contribution,
      // so that we can multiply this with the node's
      // weight.
      val (i, j) = ring.pick2(rng, 0, aperture*width)
      val a = up(i)
      val b = up(j)

      if (a.status != Status.Open || b.status != Status.Open)
        sawDown = true

      if (a.load/a.weight < b.load/b.weight) a else b
    }

    def needsRebuild: Boolean =
      sawDown || (down.nonEmpty && down.exists(nodeUp))
  }

  protected def initDistributor() =
    new Distributor(Vector.empty, 1)

  /**
   * Adjust the aperture by `n` serving units.
   */
  protected def adjust(n: Int) = invoke(_.adjust(n))
  
  /**
   * Widen the aperture by one serving unit.
   */
  protected def widen() = adjust(1)
  
  /**
   * Narrow the aperture by one serving unit.
   */
  protected def narrow() = adjust(-1)
  
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
}

/**
 * LoadBand is an aperture controller targeting a load band.
 * `lowLoad` and `highLoad` are watermarks used to adjust the
 * aperture. Whenever the the capacity-adjusted, exponentially
 * smoothed, load is less than `lowLoad`, the aperture is shrunk by
 * one serving unit; when it exceeds `highLoad`, the aperture is
 * opened by one serving unit.
 *
 * The upshot is that `lowLoad` and `highLoad` define an acceptable
 * band of load for each serving unit.
 */
private trait LoadBand[Req, Rep] { self: Balancer[Req, Rep] with Aperture[Req, Rep] =>
  /**
   * The time-smoothing factor used to compute the capacity-adjusted
   * load. Exponential smoothing is used to absorb large spikes or
   * drops. A small value is typical, usually in the order of
   * seconds.
   */
  protected def smoothWin: Duration
  
  /**
   * The lower bound of the load band. 
   * Must be less than [[highLoad]].
   */
  protected def lowLoad: Double
  
  /**
   * The upper bound of the load band.
   * Must be greater than [[lowLoad]].
   */
  protected def highLoad: Double

  private[this] val total = new AtomicInteger(0)
  private[this] val ema = new Ema(smoothWin.inNanoseconds)

  /**
   * Adjust `node`'s load by `n`.
   */
  private[this] def adjustNode(node: Node, delta: Int) = {
    node.counter.addAndGet(delta)
    val avg = ema.update(System.nanoTime(), total.addAndGet(delta))

    // Compute the capacity-adjusted average load and adjust the
    // aperture accordingly. We make only directional adjustments as
    // required, incrementing or decrementing the aperture by 1.
    //
    // Adjustments are somewhat racy: aperture and units may change
    // from underneath us. But this is not a big deal. If we
    // overshoot, the controller will self-correct quickly.
    val a = avg/aperture

    if (a >= highLoad && aperture < units)
      widen()
    else if (a <= lowLoad && aperture > 1)
      narrow()
  }

  protected case class Node(
      factory: ServiceFactory[Req, Rep], 
      weight: Double, 
      counter: AtomicInteger, token: Int)
    extends ServiceFactoryProxy[Req, Rep](factory) 
    with NodeT {
    type This = Node

    def newWeight(weight: Double) = copy(weight=weight)
    def load = counter.get
    def pending = counter.get

    override def apply(conn: ClientConnection) = {
      adjustNode(this, 1)
      super.apply(conn) transform {
        case Return(svc) =>
          Future.value(new ServiceProxy(svc) {
            override def close(deadline: Time) = 
              super.close(deadline) ensure {
                adjustNode(Node.this, -1)
              }
          })

        case t@Throw(_) =>
          adjustNode(this, -1)
          Future.const(t)
      }
    }
  }

  protected def newNode(factory: ServiceFactory[Req, Rep], weight: Double, statsReceiver: StatsReceiver) = 
    Node(factory, weight, new AtomicInteger(0), rng.nextInt())

  private[this] val failingLoad = new AtomicInteger(0)
  protected def failingNode(cause: Throwable) = Node(new FailingFactory(cause), 0D, failingLoad, 0)
}
