package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.util.Updater
import com.twitter.util.{Future, Time, Closable}
import scala.annotation.tailrec

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
   * The type of Node. Mixed in.
   */
  protected type Node <: AnyRef with NodeT[Req, Rep] { type This = Node }

  /**
   * Create a new node representing the given factory, with the given
   * weight. Report node-related stats to the given StatsReceiver.
   */
  protected def newNode(
    factory: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver
  ): Node

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

  protected sealed trait Update
  protected case class NewList(
    svcFactories: Traversable[ServiceFactory[Req, Rep]]) extends Update
  protected case class Rebuild(cur: Distributor) extends Update
  protected case class Invoke(fn: Distributor => Unit) extends Update

  private[this] val updater = new Updater[Update] {
    protected def preprocess(updates: Seq[Update]): Seq[Update] = {
      if (updates.size == 1)
        return updates

      val types = updates.reverse.groupBy(_.getClass)

      val update: Seq[Update] = types.get(classOf[NewList]) match {
        case Some(Seq(last, _*)) => Seq(last)
        case None => types.getOrElse(classOf[Rebuild], Nil).take(1)
      }

      update ++ types.getOrElse(classOf[Invoke], Nil).reverse
    }

    def handle(u: Update): Unit = u match {
      case NewList(svcFactories) =>
        val newFactories = svcFactories.toSet
        val (transfer, closed) = dist.vector.partition { node =>
          newFactories.contains(node.factory)
        }

        for (node <- closed)
          node.close()
        removes.incr(closed.size)

        // we could demand that 'n' proxies hashCode, equals (i.e. is a Proxy)
        val transferNodes = transfer.map(n => n.factory -> n).toMap
        var numNew = 0
        val newNodes = svcFactories.map {
          case f if transferNodes.contains(f) => transferNodes(f)
          case f =>
            numNew += 1
            newNode(f, statsReceiver.scope(f.toString))
        }

        dist = dist.rebuild(newNodes.toVector)
        adds.incr(numNew)

      case Rebuild(_dist) if _dist == dist =>
        dist = dist.rebuild()

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
  def update(factories: Traversable[ServiceFactory[Req, Rep]]): Unit =
    updater(NewList(factories))

  /**
   * Invoke `fn` on the current distributor. This is done through the updater
   * and is serialized with distributor updates and other invocations.
   */
  protected def invoke(fn: Distributor => Unit): Unit = {
    updater(Invoke(fn))
  }

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
