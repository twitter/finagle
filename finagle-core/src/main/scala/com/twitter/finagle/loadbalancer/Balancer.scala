package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.util.Updater
import com.twitter.util.{Closable, Future, Time}
import scala.annotation.tailrec
import scala.collection.{immutable, mutable}

/**
 * Basic functionality for a load balancer. Balancer takes care of
 * maintaining and updating a distributor, which is responsible for
 * distributing load across a number of nodes.
 *
 * This arrangement allows separate functionality to be mixed in. For
 * example, we can specify and mix in a load metric (via a Node) and
 * a balancer (a Distributor) separately.
 */
private[loadbalancer] trait Balancer[Req, Rep] extends ServiceFactory[Req, Rep] { self =>
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
   * The type of Node. Mixed in.
   */
  protected type Node <: AnyRef with NodeT[Req, Rep]

  /**
   * Create a new node representing the given factory.
   * Report node-related stats to the given StatsReceiver.
   */
  protected def newNode(
    factory: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver
  ): Node

  /**
   * Allows implementations to transform the underlying [[ServiceFactory]]
   * without having to implement a [[Node]].
   */
  protected def newFactory(
    factory: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] = factory

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

  @volatile protected var dist: Distributor = initDistributor()

  protected def rebuild(): Unit = {
    updater(Rebuild(dist))
  }

  // A counter that should be named "max_effort_exhausted".
  // Due to a scalac compile/runtime problem we were unable
  // to store it as a member variable on this trait.
  protected[this] def maxEffortExhausted: Counter

  private[this] val gauges = Seq(
    statsReceiver.addGauge("available") {
      dist.vector.count(n => n.status == Status.Open)
    },
    statsReceiver.addGauge("busy") {
      dist.vector.count(n => n.status == Status.Busy)
    },
    statsReceiver.addGauge("closed") {
      dist.vector.count(n => n.status == Status.Closed)
    },
    statsReceiver.addGauge("load") {
      dist.vector.map(_.pending).sum
    },
    statsReceiver.addGauge("size") { dist.vector.size })

  private[this] val adds = statsReceiver.counter("adds")
  private[this] val removes = statsReceiver.counter("removes")
  private[this] val rebuilds = statsReceiver.counter("rebuilds")
  private[this] val updates = statsReceiver.counter("updates")

  protected sealed trait Update
  protected case class NewList(
    svcFactories: IndexedSeq[ServiceFactory[Req, Rep]]) extends Update
  protected case class Rebuild(cur: Distributor) extends Update
  protected case class Invoke(fn: Distributor => Unit) extends Update

  private[this] val updater = new Updater[Update] {

    /**
     * We preprocess `updates` according to the following algorithm:
     *
     * 1. We collapse `NewList` updates so that the last wins.
     * 2. We collapse `Rebuild` updates so that the last wins.
     * 3. We collapse `NewList` and `Rebuild` updates that so that `NewList` wins.
     * 4. We don't collapse `Invoke` updates.
     *
     * NOTE: `Rebuild` is an internal event and may contain a stale serverset
     * while `NewList` originates from the underlying namer.
     */
    protected def preprocess(updates: Seq[Update]): Seq[Update] = {
      if (updates.size == 1) { // We know that `updates` is `ArrayBuffer`
        return updates         // so `size` is cheap.
      }

      val result: mutable.ListBuffer[Update] = mutable.ListBuffer.empty
      var listOrRebuild: Update = null // Not an option to save allocations.

      val it = updates.iterator
      while (it.hasNext) {
        it.next() match {
          case l@NewList(_) =>
            listOrRebuild = l
          case r@Rebuild(_) =>
            if (listOrRebuild == null || listOrRebuild.isInstanceOf[Rebuild]) {
              listOrRebuild = r
            }
          case i@Invoke(_) =>
            result += i
        }
      }

      if (listOrRebuild == null) result
      else listOrRebuild +=: result
    }

    private[this] val factoryToNode: Node => (ServiceFactory[Req, Rep], Node) =
      n => n.factory -> n

    private[this] val closeNode: Node => Unit = n => n.close()

    def handle(u: Update): Unit = u match {
      case NewList(newFactories) =>
        // We get a new list of service factories, and compare against the
        // current service factories in the distributor. We want to preserve
        // the existing nodes if possible. We close the set difference of new
        // factories - old factories, and rebuild the distributor with new
        // factories, preserving the nodes of the factories in the intersection.

        // We will rebuild `Distributor` with these nodes. Note, it's important
        // that we maintain the order of the `newFactories` collection as some
        // `Distributor` implementations rely on its ordering.
        val transferred: immutable.VectorBuilder[Node] = new immutable.VectorBuilder[Node]

        // These nodes are currently maintained by `Distributor`.
        val oldFactories: mutable.HashMap[ServiceFactory[Req, Rep], Node] =
          dist.vector.map(factoryToNode)(collection.breakOut)

        var numAdded: Int = 0

        for (factory <- newFactories) {
          if (oldFactories.contains(factory)) {
            transferred += oldFactories(factory)
            oldFactories.remove(factory)
          } else {
            val fact = newFactory(factory)
            transferred += newNode(fact, statsReceiver.scope(fact.toString))
            numAdded += 1
          }
        }

        // Close nodes that weren't transferred.
        oldFactories.values.foreach(closeNode)

        removes.incr(oldFactories.size)
        adds.incr(numAdded)

        dist = dist.rebuild(transferred.result())
        rebuilds.incr()

      case Rebuild(_dist) if _dist == dist =>
        dist = dist.rebuild()
        rebuilds.incr()

      case Rebuild(_stale) =>

      case Invoke(fn) =>
        fn(dist)
    }
  }

  /**
   * Update the load balancer's service list. After the update, which
   * may run asynchronously, is completed, the load balancer balances
   * across these factories and no others.
   */
  def update(factories: IndexedSeq[ServiceFactory[Req, Rep]]): Unit = {
    updates.incr()
    updater(NewList(factories))
  }

  /**
   * Invoke `fn` on the current distributor. This is done through the updater
   * and is serialized with distributor updates and other invocations.
   */
  protected def invoke(fn: Distributor => Unit): Unit =
    updater(Invoke(fn))

  @tailrec
  private[this] def pick(nodes: Distributor, count: Int): Node = {
    if (count == 0)
      return null.asInstanceOf[Node]

    val n = dist.pick()
    if (n.factory.status == Status.Open) n
    else pick(nodes, count-1)
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val d = dist

    var n = pick(d, maxEffort)
    if (n == null) {
      maxEffortExhausted.incr()
      rebuild()
      n = dist.pick()
    }

    val f = n(conn)
    if (d.needsRebuild && d == dist)
      rebuild()
    f
  }

  def close(deadline: Time): Future[Unit] = {
    for (gauge <- gauges) gauge.remove()
    removes.incr(dist.vector.size)
    Closable.all(dist.vector:_*).close(deadline)
  }
}
