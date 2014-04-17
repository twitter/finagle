package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.{Drv, Rng, Updater, Prioritized}
import com.twitter.util.{Future, Var, Time, Return, Throw, Closable}
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable

object P2CBalancerFactory extends WeightedLoadBalancerFactory {
  def newLoadBalancer[Req, Rep](
    weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep] =
    new P2CBalancer[Req,Rep](weighted, statsReceiver=statsReceiver)
}

private object P2CBalancer {
  case class Node[-Req, +Rep](
    factory: ServiceFactory[Req, Rep],
    weight: Double,
    load: AtomicInteger = new AtomicInteger(0)
  )

  val failingNode: Node[Any, Nothing] = Node(
    new FailingFactory(new NoBrokersAvailableException),
    1.0
  )
  
  /**
   * A vector of Nodes over which we load balance using biased
   * coin flipping using [[com.twitter.finagle.util.Drv Drv]].
   *
   * Unavailable nodes are assigned a weight of 0.
   */
  case class Nodes[-Req, +Rep](vector: IndexedSeq[Node[Req, Rep]], rng: Rng) 
      extends Traversable[Node[Req, Rep]]
      with Closable {
    private[this] val weights = new Array[Double](vector.size)
    private[this] var unavailable: Vector[Node[Req, Rep]] = Vector.empty

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
        
      // TODO: determine whether we want to pick according to
      // discretized weights. that is, for fractional load/weight,
      // should we flip another coin according to the implied ratio?
      val a, b = vector(drv(rng))
      if (a.weight == 0) {
        // This only happens when all weights are 0.
        if (a.load.get < b.load.get) a else b
      } else {
        if (a.load.get/a.weight < b.load.get/b.weight) a else b
      }
    }
  }

  val initNodes: Nodes[Any, Nothing] = Nodes(Vector.empty, Rng())

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
 * @param underlying the set of (node, weight) pairs over which we
 * distribute load according to the given distribution.
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
  underlying: Var[Traversable[(ServiceFactory[Req, Rep], Double)]],
  maxEffort: Int = 5,
  rng: Rng = Rng.threadLocal,
  statsReceiver: StatsReceiver = NullStatsReceiver
) extends ServiceFactory[Req, Rep] {
  import P2CBalancer._
  
  require(maxEffort > 0)

  private[this] val sizeGauge = statsReceiver.addGauge("size") { nodes.size }
  private[this] val adds = statsReceiver.counter("adds")
  private[this] val removes = statsReceiver.counter("removes")

  @volatile private[this] var nodes: Nodes[Req, Rep] = initNodes
  
  private[this] val availableGauge = statsReceiver.addGauge("available") {
    nodes.factories.count(_.isAvailable)
  }
  private[this] val loadGauge = statsReceiver.addGauge("load") {
    nodes.map(_.load.get).sum
  }

  private[this] val weightGauge = statsReceiver.addGauge("meanweight") {
    if (nodes.size == 0) 0
    else (nodes.map(_.weight).sum / nodes.size).toFloat
  }

  private[this] class Wrapped(n: Node[Req, Rep], underlying: Service[Req, Rep])
      extends ServiceProxy[Req, Rep](underlying) {
    override def close(deadline: Time) =
      super.close(deadline) ensure {
        n.load.decrementAndGet()
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

    n.load.incrementAndGet()
    val f = n.factory(conn) transform {
      case Return(s) =>
        Future.value(new Wrapped(n, s))
      case t@Throw(exc) =>
        n.load.decrementAndGet()
        Future.const(t)
    }

    if (nodes.isRevivable())
      update(Reweigh(nodes))

    f
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
          Node(f, w)
      }
      nodes = Nodes(newNodes.toIndexedSeq, rng)

      adds.incr(newList.size - transfer.size)

    case Reweigh(ns) if ns == nodes =>
      nodes = nodes.reweighted()
    
    case Reweigh(_stale) =>
  }
  
  // Start your engines!
  underlying observe { newList => update(Rebuild(newList)) }

  def close(deadline: Time) = nodes.close(deadline)
}
