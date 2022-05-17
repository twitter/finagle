package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import com.twitter.util.Time
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable

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
  private def maxEffort: Int = panicMode.maxEffort

  /**
   * The "mode" when the load balancer when the LB gives up trying to find a
   * healthy node. The LB sends the request to the last pick even if the node
   * is unhealthy.
   */
  private[loadbalancer] def panicMode: PanicMode

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
  final protected def failingNode(cause: Throwable): Node =
    newNode(new FailingEndpointFactory(cause))

  /**
   * A failing factory that always fails using the value of `emptyException`
   */
  private[loadbalancer] lazy val failingNode: Node =
    failingNode(emptyException)

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

  private[this] val lock = new ReentrantLock()

  // Can be read from any thread but all modifications must be done while holding the lock
  // to avoid things like a rebuild clobbering an update.
  @volatile protected var dist: Distributor = initDistributor()

  protected def rebuild(): Unit = {
    lock.lock()
    try doRebuild()
    finally lock.unlock()
  }

  // This is an optimization for the max-effort rebuilds. Some load balancers can
  // attempt to rebuild and that may result in a more healthy distributor. However,
  // if someone else is already rebuilding we don't want to wait.
  private[this] def tryRebuild(): Unit = {
    if (lock.tryLock()) {
      try doRebuild()
      finally lock.unlock()
    }
  }

  // must be called while holding `lock`
  private[this] def doRebuild(): Unit = {
    assert(lock.isLocked)
    val initial = dist
    val rebuildAttempt = initial.rebuild()
    if (rebuildAttempt != initial) {
      dist = rebuildAttempt
      rebuilds.incr()
    }
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

  private[this] val gauges = Seq(
    statsReceiver.addGauge("available") { numAvailable },
    statsReceiver.addGauge("busy") { numBusy },
    statsReceiver.addGauge("closed") { numClosed },
    statsReceiver.addGauge("load") { totalPending },
    statsReceiver.addGauge("size") { size }
  )

  private[this] val panicked = statsReceiver.counter("panicked")
  private[this] val adds = statsReceiver.counter("adds")
  private[this] val removes = statsReceiver.counter("removes")
  private[this] val rebuilds = statsReceiver.counter("rebuilds")
  private[this] val updates = statsReceiver.counter("updates")

  /**
   * Update the load balancer's service list. After the update, which
   * may run asynchronously, is completed, the load balancer balances
   * across these factories and no others.
   */
  def update(newFactories: IndexedSeq[EndpointFactory[Req, Rep]]): Unit = {
    lock.lock()
    try {
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
      val oldFactories = mutable.HashMap.empty[EndpointFactory[Req, Rep], Node]
      dist.vector.foreach { node => oldFactories.update(node.factory, node) }

      var numAdded: Int = 0
      for (factory <- newFactories) {
        oldFactories.remove(factory) match {
          case Some(f) =>
            transferred += f
          case None =>
            transferred += newNode(factory)
            numAdded += 1
        }
      }
      val numRemoved = oldFactories.size

      removes.incr(numRemoved)
      adds.incr(numAdded)

      // It isn't contractual that `newFactories` must have new or removed endpoints from the
      // current serverset. Lets guard against unnecessarily rebuilding here if no endpoints
      // have been added or removed.
      if (numAdded > 0 || numRemoved > 0) {
        dist = dist.rebuild(transferred.result())
        rebuilds.incr()
      }
    } finally {
      lock.unlock()
    }
  }

  /**
   * Returns `null` when `count` reaches 0.
   *
   * This indicates that we were unsuccessful in finding a Node
   * with a `Status.Open`.
   */
  @tailrec
  private[this] def pick(count: Int): ServiceFactory[Req, Rep] = {
    if (count == 0) null
    else {
      val n = dist.pick()
      if (n.status == Status.Open) n
      else pick(count - 1)
    }
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val snap = dist

    var node = pick(maxEffort)
    if (node == null) {
      panicked.incr()
      tryRebuild()
      node = dist.pick()
    }
    if (snap.eq(dist) && snap.needsRebuild) {
      // if `.needsRebuild == true` we definitely want a rebuild and even
      // with `tryRebuild` we will get it:
      //   - if we get the lock then we will rebuild
      //   - if we fail to get the lock it means that someone else is currently
      //     rebuilding and `snap` will be replaced by them.
      tryRebuild()
    }

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
