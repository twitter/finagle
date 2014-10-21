package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, Counter}
import com.twitter.finagle.util.{OnReady, Drv, Rng, Updater, Prioritized}
import com.twitter.util.{Activity, Future, Promise, Time, Closable, Return, Throw}
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable

/**
 * Basic functionality for a load balancer. Balancer takes care of
 * maintaining and updating a distributor, which is responsible for
 * distributing load across a number of nodes.
 *
 * This arrangement allows separate functionality to be mixed in. For
 * example, we can specify and mix in a load metric (via a Node) and
 * a balancer (a Distributor) separately.
 */
private trait Balancer[Req, Rep] extends ServiceFactory[Req, Rep] { self =>
  /** 
   * The maximum number of balancing tries (yielding unavailable
   * factories) until we give up.
   */
  protected def maxEffort: Int

  /**
   * The throwable to use when the load balancer is empty.
   */
  protected def emptyException: Throwable
  protected lazy val empty = Future.exception(emptyException)

  /**
   * Balancer reports stats here.
   */
  protected def statsReceiver: StatsReceiver
  
  /**
   * The base type of nodes over which load is balanced.
   * Nodes define the load metric that is used; distributors
   * like P2C will use these to decide where to balance
   * the next connection request.
   */
  protected trait NodeT extends ServiceFactory[Req, Rep] {
    type This
    
    /**
     * The current load, in units of the active metric.
     */
    def load: Double

    /**
     * The number of pending requests to this node.
     */
    def pending: Int

    /**
     * This node's weight.
     */
    def weight: Double
    
    /**
     * Nondestructively update this node with the supplied weight.
     */  
    def newWeight(weight: Double): This
    
    /**
     * The underlying service factory.
     */
    def factory: ServiceFactory[Req, Rep]
  }
  
  /**
   * The type of Node. Mixed in.
   */
  protected type Node <: AnyRef with NodeT { type This = Node }
  
  /**
   * Create a new node representing the given factory, with the given
   * weight. Report node-related stats to the given StatsReceiver.
   */
  protected def newNode(factory: ServiceFactory[Req, Rep], 
    weight: Double, statsReceiver: StatsReceiver): Node
  
  /**
   * Create a node whose sole purpose it is to endlessly fail
   * with the given cause.
   */
  protected def failingNode(cause: Throwable): Node
  
  /**
   * The base type of the load balancer distributor. Distributors are
   * updated nondestructively, but, as with nodes, may share some
   * data across updates.
  */
  protected trait DistributorT {
    type This
    
    /**
     * The vector of nodes over which we are currently balancing.
     */
    def vector: Vector[Node]
    
    /**
     * Pick the next node. This is the main load balancer.
     */
    def pick(): Node
    
    /**
     * True if this distributor needs to be rebuilt. (For example, it
     * may need to be updated with current availabilities.)
     */
    def needsRebuild: Boolean
    
    /**
     * Rebuild this distributor.
     */
    def rebuild(): This
  }
  
  /**
   * The type of Distributor. Mixed in.
   */
  protected type Distributor <: DistributorT { type This = Distributor }
  
  /**
   * Create a fresh Distributor to load balance over the vector of nodes.
   * Surviging nodes persist across updates; they are not created anew.
   */
  protected def newDistributor(nodes: Vector[Node]): Distributor

  @volatile protected var dist: Distributor = newDistributor(Vector.empty)

  private[this] val gauges = Seq(
    statsReceiver.addGauge("available") {
      dist.vector.count(_.isAvailable)
    },
    statsReceiver.addGauge("load") {
      dist.vector.map(_.pending).sum
    },
    statsReceiver.addGauge("meanweight") {
      if (dist.vector.size == 0) 0
      else (dist.vector.map(_.weight).sum / dist.vector.size).toFloat
    },
    statsReceiver.addGauge("size") { dist.vector.size })

  private[this] val adds = statsReceiver.counter("adds")
  private[this] val removes = statsReceiver.counter("removes")

  protected sealed trait Update
  protected case class NewList(
      newList: Traversable[(ServiceFactory[Req, Rep], Double)])
    extends Update
  protected case class Rebuild(cur: Distributor) extends Update
  protected object Update {
    implicit def pri = new Prioritized[Update] {
      def apply(u: Update) = u match {
        case NewList(_) => 0
        case Rebuild(_) => 1
      }
    }
  }

  private[this] val updater = Updater[Update] {
    case NewList(newList) =>
      val newFactories = (newList map { case (f, _) => f }).toSet
      val (transfer, closed) = dist.vector partition (newFactories contains _.factory)

      for (node <- closed)
        node.close()
      removes.incr(closed.size)

      // we could demand that 'n' proxies hashCode, equals (i.e. is a Proxy)
      val transferNodes = transfer.map(n => n.factory -> n).toMap
      val newNodes = newList map {
        case (f, w) if transferNodes contains f =>
          transferNodes(f).newWeight(w)
        case (f, w) =>
          newNode(f, w, statsReceiver.scope(f.toString))
      }

      dist = newDistributor(newNodes.toVector)
      adds.incr(newList.size - transfer.size)

    case Rebuild(_dist) if _dist == dist =>
      dist = dist.rebuild()

    case Rebuild(_stale) =>
  }
  
  /**
   * Update the load balancer's service list. After the update, which
   * may run asynchronously, is completed, the load balancer balances
   * across these factories and no others.
   */
  def update(factories: Traversable[(ServiceFactory[Req, Rep], Double)]): Unit =
    updater(NewList(factories))

  @tailrec
  private[this] def pick(nodes: Distributor, count: Int): Node = {
    if (count == 0)
      return null.asInstanceOf[Node]

    val n = dist.pick()
    if (n.factory.isAvailable) n
    else pick(nodes, count-1)
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val d = dist

    var n = pick(d, maxEffort)
    if (n == null) {
      updater(Rebuild(dist))
      n = dist.pick()
    }

    val f = n(conn)
    if (d.needsRebuild && d == dist)
      updater(Rebuild(d))
    f
  }

  def close(deadline: Time) =
    Closable.all(dist.vector:_*).close(deadline)
}

/**
 * A Balancer mix-in to provide automatic updating via Activities.
 */
private trait Updating[Req, Rep] extends OnReady { self: Balancer[Req, Rep] =>
  private[this] val ready = new Promise[Unit]
  def onReady: Future[Unit] = ready

  /**
   * An activity representing the active set of ServiceFactories.
   */
  protected def activity: Activity[Traversable[(ServiceFactory[Req, Rep], Double)]]

  /*
   * Subscribe to the Activity and dynamically update the load
   * balancer as it (succesfully) changes.
   *
   * The observation is terminated when the Balancer is closed.
   */
  private[this] val observation = activity.run.changes respond {
    case Activity.Pending =>

    case Activity.Ok(newList) =>
      update(newList)
      ready.setDone()

    case Activity.Failed(_) =>
      // On resolution failure, consider the 
      // load balancer ready (to serve errors).
      ready.setDone()
  }

  override def close(deadline: Time): Future[Unit] = {
    observation.close(deadline) transform { _ => self.close(deadline) } ensure {
      ready.setDone()
    }
  }
}

/**
 * Provide Nodes whose 'load' is the current number of pending
 * requests and thus will result in least-loaded load balancer.
 */
private trait LeastLoaded[Req, Rep] { self: Balancer[Req, Rep] =>
  protected case class Node(factory: ServiceFactory[Req, Rep], weight: Double, counter: AtomicInteger)
      extends ServiceFactoryProxy[Req, Rep](factory) 
      with NodeT {

    type This = Node

    def newWeight(weight: Double) = copy(weight=weight)
    def load = counter.get
    def pending = counter.get

    override def apply(conn: ClientConnection) = {
      counter.incrementAndGet()
      super.apply(conn) transform {
        case Return(svc) =>
          Future.value(new ServiceProxy(svc) {
            override def close(deadline: Time) = 
              super.close(deadline) ensure {
                counter.decrementAndGet()
              }
          })

        case t@Throw(_) =>
          counter.decrementAndGet()
          Future.const(t)
      }
    }
  }

  protected def newNode(factory: ServiceFactory[Req, Rep], weight: Double, statsReceiver: StatsReceiver) = 
    Node(factory, weight, new AtomicInteger(0))

  private[this] val failingLoad = new AtomicInteger(0)
  protected def failingNode(cause: Throwable) = Node(new FailingFactory(cause), 0D, failingLoad)
}

/**
 * An O(1), concurrent, weighted fair load balancer. This uses the
 * ideas behind "power of 2 choices" [1] combined with O(1) biased
 * coin flipping through the aliasing method, described in
 * [[com.twitter.finagle.util.Drv Drv]].
 *
 * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
 * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
 * 10 (October 2001), 1094-1104.
 */
private trait P2C[Req, Rep] { self: Balancer[Req, Rep] =>
  /**
   * Our sturdy coin flipper.
   */
  protected def rng: Rng

  private[this] val nodeAvail: Node => Boolean = _.isAvailable

  protected case class Distributor(vector: Vector[Node]) extends DistributorT {
    type This = Distributor

    private[this] val weights = new Array[Double](vector.size)
    
    // Build weights, 
    private[this] val unavailable = {
      val downbuild = new immutable.VectorBuilder[Node]()
      for (i <- vector.indices) {
        if (vector(i).factory.isAvailable) {
          weights(i) = vector(i).weight
        } else {
          weights(i) = 0
          downbuild += vector(i)
        }
      }

      downbuild.result()
    }
    
    private[this] val drv = 
      if (weights.isEmpty) Drv(Vector.empty)
      else Drv.fromWeights(weights)

    def needsRebuild = 
      unavailable.nonEmpty && unavailable.exists(nodeAvail)

    def rebuild() = copy()

    def pick(): Node = {
      if (vector.isEmpty)
        return failingNode(emptyException)
        
      if (vector.size == 1)
        return vector(0)

      // TODO: determine whether we want to pick according to
      // discretized weights. that is, for fractional load/weight,
      // should we flip another coin according to the implied ratio?
      val a = vector(drv(rng))
      var b = a
      var i = 0
      // Try to pick b, b != a, up to 10 times. This is mostly to
      // account for pathological cases where there is only one
      // realistically pickable element in the vector (e.g. where
      // other weights are 0 or close to 0).
      do {
        b = vector(drv(rng))
        i += 1
      } while (a == b && i < 10)
  
      if (a.weight == 0.0) {
        // This only happens when all weights are 0.
        if (a.load < b.load) a else b
      } else {
        if (a.load/a.weight < b.load/b.weight) a else b
      }
    }
  }
  
  protected def newDistributor(vector: Vector[Node]) = Distributor(vector)
}
