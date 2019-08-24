package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.util.{Future, Time}
import scala.annotation.tailrec
import scala.collection.{immutable, mutable}

/**
 * A [[BalancerNode]] allows implementors to refine the node type-alias
 * without eagerly mixing in all of [[Balancer]].
 */
private trait BalancerNode[Req, Rep] { self: Balancer[Req, Rep] =>
  protected type Node <: NodeT[Req, Rep]
}

/**
 * Basic functionality for a load balancer. Balancer takes care of
 * maintaining and updating a distributor, which is responsible for
 * distributing load across a number of nodes.
 *
 * This arrangement allows separate functionality to be mixed in. For
 * example, we can specify and mix in a load metric (via a `Node`) and
 * a balancer (a `Distributor`) separately.
 */
private trait Balancer[Req, Rep] extends ServiceFactory[Req, Rep] with BalancerNode[Req, Rep] {

  /**
   * Registers this balancer with the global balancer registry.
   * Nothing prevents multiple registrations with different labels.
   *
   * @param label the client's label, as provided by the [[param.Label]]
   *              Stack Param.
   * @note unregistration is handled by [[close]].
   */
  def register(label: String): Unit =
    BalancerRegistry.get.register(label, this)

  /**
   * The maximum number of balancing tries (yielding unavailable
   * factories) until we give up.
   */
  protected def maxEffort: Int

  /**
   * The throwable to use when the load balancer is empty.
   */
  protected def emptyException: Throwable

  /**
   * Balancer reports stats here.
   */
  protected def statsReceiver: StatsReceiver

  /**
   * Create a new node representing the given factory.
   */
  protected def newNode(factory: EndpointFactory[Req, Rep]): Node

  /**
   * Create a node whose sole purpose it is to endlessly fail
   * with the given cause.
   */
  protected def failingNode(cause: Throwable): Node

  /**
   * The type of Distributor. Mixed in.
   */
  protected type Distributor <: DistributorT[Node] { type This = Distributor }

  /**
   * Create an initial distributor.
   */
  protected def initDistributor(): Distributor

  /**
   * Balancer status is the best of its constituent nodes.
   */
  override def status: Status = Status.bestOf(dist.vector, nodeStatus)

  private[this] val nodeStatus: Node => Status = _.factory.status

  // Can be read from any thread but all modifications must be done while synchronized on
  // `this` to avoid things like a rebuild clobbering an update.
  @volatile protected var dist: Distributor = initDistributor()

  protected def rebuild(): Unit = {
    val initial = dist
    // since all rebuilds/updates grab the lock, we know that once we have the lock nobody
    // else is rebuilding or updating. We can then make sure that we are still rebuilding
    // what we think we're rebuilding to avoid unhelpful and costly rebuilds. Empirically
    // this hasn't been observed but we do it anyway since it's easy to check.
    val rebuilt = synchronized {
      if (initial != dist) false // someone else rebuild for us so no need for us to do so
      else {
        dist = dist.rebuild()
        true
      }
    }

    if (rebuilt) rebuilds.incr()
  }

  def numAvailable: Int =
    dist.vector.count(n => n.status == Status.Open)

  def numBusy: Int =
    dist.vector.count(n => n.status == Status.Busy)

  def numClosed: Int =
    dist.vector.count(n => n.status == Status.Closed)

  def totalPending: Int =
    dist.vector.map(_.pending).sum

  def totalLoad: Double =
    dist.vector.map(_.load).sum

  def size: Int =
    dist.vector.size

  // A counter that should be named "max_effort_exhausted".
  // Due to a scalac compile/runtime problem we were unable
  // to store it as a member variable on this trait.
  protected[this] def maxEffortExhausted: Counter

  private[this] val gauges = Seq(
    statsReceiver.addGauge("available") { numAvailable },
    statsReceiver.addGauge("busy") { numBusy },
    statsReceiver.addGauge("closed") { numClosed },
    statsReceiver.addGauge("load") { totalPending },
    statsReceiver.addGauge("size") { size }
  )

  private[this] val adds = statsReceiver.counter("adds")
  private[this] val removes = statsReceiver.counter("removes")
  private[this] val rebuilds = statsReceiver.counter("rebuilds")
  private[this] val updates = statsReceiver.counter("updates")

  private[this] val factoryToNode: Node => (EndpointFactory[Req, Rep], Node) =
    n => n.factory -> n

  /**
   * Update the load balancer's service list. After the update, which
   * may run asynchronously, is completed, the load balancer balances
   * across these factories and no others.
   */
  def update(newFactories: IndexedSeq[EndpointFactory[Req, Rep]]): Unit = synchronized {
    updates.incr()
    // We get a new list of service factories, and compare against the
    // current service factories in the distributor. We want to preserve
    // the existing nodes if possible. We rebuild the distributor with new
    // factories, preserving the nodes of the factories in the intersection.

    // We will rebuild `Distributor` with these nodes. Note, it's important
    // that we maintain the order of the `newFactories` collection as some
    // `Distributor` implementations rely on its ordering.
    val transferred: immutable.VectorBuilder[Node] = new immutable.VectorBuilder[Node]

    // These nodes are currently maintained by `Distributor`.
    val oldFactories: mutable.HashMap[EndpointFactory[Req, Rep], Node] =
      mutable.HashMap(dist.vector.map(factoryToNode): _*)

    var numAdded: Int = 0

    for (factory <- newFactories) {
      if (oldFactories.contains(factory)) {
        transferred += oldFactories(factory)
        oldFactories.remove(factory)
      } else {
        transferred += newNode(factory)
        numAdded += 1
      }
    }

    removes.incr(oldFactories.size)
    adds.incr(numAdded)

    dist = dist.rebuild(transferred.result())
    rebuilds.incr()
  }

  /**
   * Returns `null` when `count` reaches 0.
   *
   * This indicates that we were unsuccessful in finding a Node
   * with a `Status.Open`.
   */
  @tailrec
  private[this] def pick(count: Int): Node = {
    if (count == 0)
      return null.asInstanceOf[Node]

    val n = dist.pick()
    if (n.factory.status == Status.Open) n
    else pick(count - 1)
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val snap = dist

    var node = pick(maxEffort)
    if (node == null) {
      maxEffortExhausted.incr()
      rebuild()
      node = dist.pick()
    }
    if (snap.eq(dist) && snap.needsRebuild)
      rebuild()

    node(conn)
  }

  /**
   * Any additional metadata specific to a balancer implementation.
   *
   * @see [[Metadata]] and [[BalancerRegistry]]
   */
  def additionalMetadata: Map[String, Any]

  def close(deadline: Time): Future[Unit] = {
    BalancerRegistry.get.unregister(this)
    for (gauge <- gauges) gauge.remove()
    removes.incr(dist.vector.size)
    // Note, we don't treat the endpoints as a
    // resource that the load balancer owns, and as
    // such we don't close them here. We expect the
    // layers above to manage them accordingly.
    Future.Done
  }
}
