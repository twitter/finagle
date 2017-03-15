package com.twitter.finagle.loadbalancer.roundrobin

import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.{ClientConnection, NoBrokersAvailableException, Service,
  ServiceFactory, ServiceFactoryProxy, Status}
import com.twitter.util.{Activity, Future, Time}
import java.util.concurrent.atomic.AtomicLong
import com.twitter.finagle.loadbalancer.{Balancer, DistributorT, NodeT, Updating}

/**
 * A simple round robin balancer that chooses the next backend in
 * the list for each request. This balancer doesn't take a node's
 * load into account and as such doesn't mix-in a load metric.
 */
private[loadbalancer] class RoundRobinBalancer[Req, Rep](
    protected val endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
    protected val statsReceiver: StatsReceiver,
    protected val emptyException: NoBrokersAvailableException,
    protected val maxEffort: Int = 5)
  extends ServiceFactory[Req, Rep]
  with Balancer[Req, Rep]
  with Updating[Req, Rep] {

  protected[this] val maxEffortExhausted: Counter =
    statsReceiver.counter("max_effort_exhausted")

  protected class Node(val factory: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy[Req, Rep](factory)
    with NodeT[Req, Rep] {
    // Note: These stats are never updated.
    def load: Double = 0.0
    def pending: Int = 0
    def token: Int = 0

    override def close(deadline: Time): Future[Unit] = factory.close(deadline)
    override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = factory(conn)
  }

  /**
   * A simple round robin distributor.
   */
  protected class Distributor(vector: Vector[Node])
    extends DistributorT[Node](vector) {
    type This = Distributor

    /**
     * Indicates if we've seen any down nodes during `pick` which
     * we expected to be available
     */
    @volatile
    protected[this] var sawDown = false

    /**
     * `up` is the Vector of nodes that were `Status.Open` at creation time.
     * `down` is the Vector of nodes that were not `Status.Open` at creation time.
     *
     *  Since RR is not probabilistic in its selection, we're relying on partition
     *  and select only from healthy nodes.
     */
    private[this] val (up: Vector[Node], down: Vector[Node]) =
      vector.partition(_.isAvailable)

    /**
     * Utility used by [[pick()]].
     *
     * If all nodes are down, we might as well try to send requests somewhere
     * as our view of the world may be out of date.
     */
    protected[this] val selections: Vector[Node] =
      if (up.isEmpty) down else up

    private[this] val currentNode = new AtomicLong()

    // For each node that's requested, we move the currentNode index
    // around the wheel using mod arithmetic. This is the round robin
    // of our balancer.
    private def chooseNext(vecSize: Int): Int = {
      val next = currentNode.getAndIncrement()
      math.abs(next % vecSize).toInt
    }

    def pick(): Node = {
      if (vector.isEmpty)
        return failingNode(emptyException)

      val node = selections(chooseNext(selections.size))
      if (node.status != Status.Open)
        sawDown = true
      node
    }

    def rebuild(): This = new Distributor(vector)
    def rebuild(vector: Vector[Node]): This = new Distributor(vector)

    def needsRebuild: Boolean = {
      // while the `nonEmpty` check isn't necessary, it is an optimization
      // to avoid the iterator allocation in the common case where `down`
      // is empty.
      sawDown || (down.nonEmpty && down.exists(_.isAvailable))
    }
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty)

  protected def newNode(
    factory: ServiceFactory[Req,Rep],
    statsReceiver: StatsReceiver
  ): Node = new Node(factory)

  protected def failingNode(cause: Throwable): Node = new Node(new FailingFactory(cause))
}