package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.{OnReady, Rng, Updater}
import com.twitter.util.{Activity, Future, Promise, Time, Closable, Return, Throw}
import java.util.concurrent.atomic.AtomicInteger
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
     * A token is a random integer identifying the node.
     * It persists through node updates.
     */
    def token: Int

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

    /**
     * Rebuild this distributor with a new vector.
     */
    def rebuild(vector: Vector[Node]): This
  }

  /**
   * The type of Distributor. Mixed in.
   */
  protected type Distributor <: DistributorT { type This = Distributor }

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

/**
 * A Balancer mix-in to provide automatic updating via Activities.
 */
private trait Updating[Req, Rep] extends Balancer[Req, Rep] with OnReady {
  private[this] val ready = new Promise[Unit]
  def onReady: Future[Unit] = ready

  /**
   * An activity representing the active set of ServiceFactories.
   */
  protected def activity: Activity[Traversable[ServiceFactory[Req, Rep]]]

  /*
   * Subscribe to the Activity and dynamically update the load
   * balancer as it (succesfully) changes.
   *
   * The observation is terminated when the Balancer is closed.
   */
  private[this] val observation = activity.states.respond {
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
    observation.close(deadline) transform { _ => super.close(deadline) } ensure {
      ready.setDone()
    }
  }
}

/**
 * Provide Nodes whose 'load' is the current number of pending
 * requests and thus will result in least-loaded load balancer.
 */
private trait LeastLoaded[Req, Rep] { self: Balancer[Req, Rep] =>
  protected def rng: Rng

  protected case class Node(factory: ServiceFactory[Req, Rep], counter: AtomicInteger, token: Int)
    extends ServiceFactoryProxy[Req, Rep](factory)
    with NodeT {

    type This = Node

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

  protected def newNode(factory: ServiceFactory[Req, Rep], statsReceiver: StatsReceiver) =
    Node(factory, new AtomicInteger(0), rng.nextInt())

  private[this] val failingLoad = new AtomicInteger(0)
  protected def failingNode(cause: Throwable) = Node(new FailingFactory(cause), failingLoad, 0)
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

  protected class Distributor(val vector: Vector[Node]) extends DistributorT {
    type This = Distributor

    private[this] val nodeUp: Node => Boolean = { node =>
      node.status == Status.Open
    }

    private[this] val (up, down) = vector.partition(nodeUp)

    def needsRebuild: Boolean = down.nonEmpty && down.exists(nodeUp)
    def rebuild(): This = new Distributor(vector)
    def rebuild(vec: Vector[Node]): This = new Distributor(vec)

    def pick(): Node = {
      if (vector.isEmpty)
        return failingNode(emptyException)

      // if all nodes are down, we might as well try to send requests somewhere
      // as our view of the world may be out of date.
      val vec = if (up.isEmpty) down else up
      val size = vec.size

      if (size == 1) vec.head else {
        val a = rng.nextInt(size)
        var b = rng.nextInt(size)

        // Try to pick b, b != a, up to 10 times.
        var i = 10
        while (a == b && i > 0) {
          b = rng.nextInt(size)
          i -= 1
        }

        val nodeA = vec(a)
        val nodeB = vec(b)
        if (nodeA.load < nodeB.load) nodeA else nodeB
      }
    }
  }

  protected def initDistributor() = new Distributor(Vector.empty)
}

