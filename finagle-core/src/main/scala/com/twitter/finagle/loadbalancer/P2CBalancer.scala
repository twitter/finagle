package com.twitter.finagle.loadbalancer

import com.twitter.app.GlobalFlag
import com.twitter.finagle._
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.{OnReady, Drv, Rng, Updater, Prioritized}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.immutable

package exp {
  object loadMetric extends GlobalFlag("leastReq", "Metric used to measure load across endpoints (leastReq | pendingEwmaMs)")
}

object P2CBalancerFactory extends WeightedLoadBalancerFactory {
  def newLoadBalancer[Req, Rep](
    factories: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] =
    new P2CBalancer[Req, Rep](
      Activity(factories map(Activity.Ok(_))),
      statsReceiver = statsReceiver,
      emptyException = emptyException)

  def newWeightedLoadBalancer[Req, Rep](
    activity: Activity[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] =
    new P2CBalancer[Req, Rep](activity,
      statsReceiver = statsReceiver,
      emptyException = emptyException)
}

private object P2CBalancer {
  trait LoadMetric {
    /**
     * Returns the instantaneous load which determines how
     * the load balancer schedules work on this node.
     */
    def get(): Double

    /**
     * Called when the load balancer schedules a request
     * on this node. The method is expected to return an
     * identifying handle on the request.
     */
    def start(): Long

    /**
     * Called when the request associated with the handle
     * has returned.
     */
    def end(h: Long): Unit

    /**
     * Returns the load rate of this node. This is usually
     * a dimensionless quantity that can be expressed as
     * a count.
     */
    def rate(): Int
  }

  object LoadMetric {
    val MaxValue = new LoadMetric {
      def start() = 0
      def end(by: Long) = {}
      def rate() = Int.MaxValue
      def get() = Double.MaxValue
    }

    def leastReq() = new LoadMetric {
      private[this] val load = new AtomicInteger(0)
      def start() = { load.incrementAndGet(); 1 }
      def end(by: Long) = load.getAndAdd(-by.toInt)
      def rate() = load.get()
      def get() = load.get().toDouble
    }

    // temporary logger to help debug ewma load metric
    import java.util.logging.{Logger, Level}
    private val epoch = System.nanoTime()
    private val log = Logger.getLogger("com.twitter.finagle.loadbalancer.loadMetric")

    /**
     * A load metric designed to quickly converge in the face of slow
     * endpoints. Load is determined by oustanding nanos while taking
     * into account an endpoints latency history. The algorithm is
     * designed to quickly react to latency spikes and cautiously recover
     * from them.
     */
    def ewma(sr: StatsReceiver, name: String) = new LoadMetric {
      private[this] val Penalty: Double = Double.MaxValue/2
      private[this] val Sec: Long = 1000000000 // in nanos
      // The mean lifetime that an rtt observation is weighted
      // as part of the `cost`. A observation reaches its half-life
      // after Tau*ln(2).
      private[this] val Tau: Long = 2*Sec

      // these are all guarded by synchronization on `this`
      private[this] var stamp: Long = 0L        // last timestamp in nanos we updated
      private[this] var lastRep: Long = 0L      // last timestamp in nanos we received a response
      private[this] var pendingReq: Int = 0     // rate nanos/nanos at time t
      private[this] var pendingTime: Long = 0L  // pending load in nanos
      private[this] var cost: Double = 0.0      // ewma of rtt

      private[this] val loadGauge = sr.addGauge("loadms") { (get()/1000000).toFloat }

      @inline private[this] def update() {
        val t1 = System.nanoTime()
        pendingTime += (t1-stamp)*pendingReq
        stamp = t1
      }

      def rate(): Int = synchronized { pendingReq }

      def get(): Double = synchronized {
        update()
        // We assume the worst when we don't have any historical
        // data but have outstanding requests (i.e. we penalize the host).
        // To avoid arbitrarily balancing across an influx of penalized hosts,
        // we also factor in our outstanding load.
        if (cost == 0.0 && pendingReq != 0) Penalty+pendingTime
        else {
          // Consider the load if we were to schedule an additional request
          // on to this host.
          val pending = if (pendingReq == 0) 0 else pendingTime+(pendingTime/pendingReq)
          math.max(cost*(pendingReq+1.0), pending.toDouble)
        }
      }

      def start(): Long = synchronized {
        update()
        pendingReq += 1
        stamp
      }

      def end(ts: Long) = synchronized {
        update()
        val rtt = stamp-ts
        val td = stamp-lastRep
        // `rtt` decays by 1/e for every Tau that elapses
        val weight = math.exp(-td/Tau)
        lastRep = stamp
        pendingReq -= 1
        pendingTime -= rtt
        // set a reasonable initial cost
        if (cost == 0.0) cost = rtt
        else cost = cost*weight + (1.0-weight)*rtt

        if (log.isLoggable(Level.FINEST)) {
          log.finest("[%s] clock=%d, rtt=%d, cost=%f, pendingTime=%d".format(
            name, (stamp-epoch), rtt, cost, pendingTime))
        }
      }
    }
  }

  case class Node[-Req, +Rep](
    factory: ServiceFactory[Req, Rep],
    weight: Double,
    load: LoadMetric
  )

  /**
   * A vector of Nodes over which we load balance using biased
   * coin flipping using [[com.twitter.finagle.util.Drv Drv]].
   *
   * Unavailable nodes are assigned a weight of 0.
   */
  case class Nodes[-Req, +Rep](
    vector: IndexedSeq[Node[Req, Rep]],
    rng: Rng,
    emptyException: NoBrokersAvailableException
  ) extends Traversable[Node[Req, Rep]] with Closable {
    private[this] val weights = new Array[Double](vector.size)
    private[this] var unavailable: Vector[Node[Req, Rep]] = Vector.empty

    private[this] lazy val failingNode: Node[Any, Nothing] = Node(
      new FailingFactory(emptyException),
      1.0,
      LoadMetric.MaxValue
    )

    { // Build the weight and down vectors.
      val downbuild = new immutable.VectorBuilder[Node[Req, Rep]]()
      for (i <- vector.indices) {
        if (vector(i).factory.isAvailable) {
          weights(i) = vector(i).weight
        } else {
          weights(i) = 0
          downbuild += vector(i)
        }
      }

      unavailable = downbuild.result()
    }

    private[this] val drv = Drv.fromWeights(weights)
    private[this] val nodeAvailable: Node[Req, Rep] => Boolean = _.factory.isAvailable

    def foreach[T](f: Node[Req, Rep] => T) = vector.foreach(f)
    override val size = vector.size

    /**
     * Are there any revivable nodes in the down-set?
     */
    def isRevivable() = unavailable.nonEmpty && unavailable.exists(nodeAvailable)

    /**
     * Reweight this Node vector. This returns a new Nodes instance
     * with rebuilt weight and unavailable vectors.
     */
    def reweighted(): Nodes[Req, Rep] = copy()

    def factories: Seq[ServiceFactory[Req, Rep]] = vector map (_.factory)

    def close(deadline: Time) = Closable.all(factories:_*).close(deadline)

    /**
     * Pick least-of-two according to load and weight.
     */
    def pick2(): Node[Req, Rep] = {
      if (vector.isEmpty)
        return failingNode

      if (vector.size == 1)
        return vector(0)

      // TODO: determine whether we want to pick according to
      // discretized weights. that is, for fractional load/weight,
      // should we flip another coin according to the implied ratio?
      val a = vector(drv(rng))
      var b: Node[Req, Rep] = null
      var i = 0
      // Try to pick b, b != a, up to 10 times. This is mostly to
      // account for pathological cases where there is only one
      // realistically pickable element in the vector (e.g. where
      // other weights are 0 or close to 0).
      do {
        b = vector(drv(rng))
        i += 1
      } while (a == b && i < 10)

      if (a.weight == 0) {
        // This only happens when all weights are 0.
        if (a.load.get() < b.load.get()) a else b
      } else {
        if (a.load.get()/a.weight < b.load.get()/b.weight) a else b
      }
    }
  }

  /**
   * Operations representing updates to the node vectors.
   */
  sealed trait Update[Req, Rep] { val pri: Int }
  case class Rebuild[Req, Rep](newList: Traversable[(ServiceFactory[Req, Rep], Double)])
      extends Update[Req, Rep] { val pri = 0 }
  case class Reweigh[Req, Rep](cur: Nodes[Req, Rep])
      extends Update[Req, Rep] { val pri = 1 }
  object Update {
    implicit def pri[Req, Rep] = new Prioritized[Update[Req, Rep]] {
      def apply(u: Update[Req, Rep]) = u.pri
    }
  }
}

/**
 * An O(1), concurrent, weighted fair load balancer. This uses the
 * ideas behind "power of 2 choices" [1] combined with O(1) biased
 * coin flipping through the aliasing method, described in
 * [[com.twitter.finagle.util.Drv Drv]].
 *
 * @param underlying An activity that updates with the set of
 * (node, weight) pairs over which we distribute load.
 *
 * @param maxEffort the maximum amount of "effort" we're willing to
 * expend on a load balancing decision without reweighing.
 *
 * @param rng The PRNG used for flipping coins. Override for
 * deterministic tests.
 *
 * @param statsReceiver The stats receiver to which operational
 * statistics are reported.
 *
 * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
 * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
 * 10 (October 2001), 1094-1104.
 */
class P2CBalancer[Req, Rep](
  underlying: Activity[Traversable[(ServiceFactory[Req, Rep], Double)]],
  maxEffort: Int = 5,
  rng: Rng = Rng.threadLocal,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  emptyException: NoBrokersAvailableException = new NoBrokersAvailableException
) extends ServiceFactory[Req, Rep] with OnReady {

  import P2CBalancer._

  require(maxEffort > 0)

  private[this] val ready = new Promise[Unit]
  def onReady: Future[Unit] = ready

  private[this] val sizeGauge = statsReceiver.addGauge("size") { nodes.size }
  private[this] val adds = statsReceiver.counter("adds")
  private[this] val removes = statsReceiver.counter("removes")

  @volatile private[this] var nodes: Nodes[Req, Rep] =
    Nodes(Vector.empty, rng, emptyException)

  private[this] val availableGauge = statsReceiver.addGauge("available") {
    nodes.factories.count(_.isAvailable)
  }

  private[this] val loadGauge = statsReceiver.addGauge("load") {
    nodes.map(_.load.rate()).sum
  }

  private[this] val weightGauge = statsReceiver.addGauge("meanweight") {
    if (nodes.size == 0) 0
    else (nodes.map(_.weight).sum / nodes.size).toFloat
  }

  private[this] class Wrapped(n: Node[Req, Rep], underlying: Service[Req, Rep], handle: Long)
      extends ServiceProxy[Req, Rep](underlying) {
    override def close(deadline: Time) =
      super.close(deadline) ensure {
        n.load.end(handle)
      }
  }

  @tailrec
  private[this] def pick(nodes: Nodes[Req, Rep], count: Int): Node[Req, Rep] = {
    if (count == 0)
      return null

    val n = nodes.pick2()
    if (n.factory.isAvailable) n
    else pick(nodes, count-1)
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val ns = nodes
    var n = pick(ns, maxEffort)
    if (n == null) {
      update(Reweigh(ns))
      n = nodes.pick2()
    }

    val handle = n.load.start()
    val f = n.factory(conn) transform {
      case Return(s) =>
        Future.value(new Wrapped(n, s, handle))
      case t@Throw(exc) =>
        n.load.end(handle)
        Future.const(t)
    }

    if (nodes.isRevivable())
      update(Reweigh(nodes))

    f
  }

  private[this] val useEwma = exp.loadMetric() match {
    case "pendingEwmaMs" => true
    case _ => false
  }

  private[this] val update = Updater[Update[Req, Rep]] {
    case Rebuild(newList) =>
      val newFactories = (newList map { case (f, _) => f }).toSet
      val (transfer, closed) = nodes.vector partition (newFactories contains _.factory)

      for (Node(factory, _, _) <- closed)
        factory.close()
      removes.incr(closed.size)

      val transferNodes = Map() ++ (transfer map { n => n.factory -> n })
      val newNodes = newList map {
        case (f, w) if transferNodes contains f =>
          transferNodes(f).copy(weight=w)
        case (f, w) =>
          val metric =
            if (useEwma) LoadMetric.ewma(statsReceiver.scope(f.toString), f.toString)
            else LoadMetric.leastReq()
          Node(f, w, metric)
      }
      nodes = Nodes(newNodes.toIndexedSeq, rng, emptyException)

      adds.incr(newList.size - transfer.size)

    case Reweigh(ns) if ns == nodes =>
      nodes = nodes.reweighted()

    case Reweigh(_stale) =>
  }

  // Start your engines!
  private[this] val observation = underlying.run.changes respond {
    case Activity.Pending =>

    case Activity.Ok(newList) =>
      update(Rebuild(newList))
      ready.setDone()

    case Activity.Failed(_) =>
      // On resolution failure, consider the load balancer ready (to serve errors).
      ready.setDone()
  }

  def close(deadline: Time) =
    Closable.sequence(observation, Closable.make(nodes.close)).close(deadline)
}
