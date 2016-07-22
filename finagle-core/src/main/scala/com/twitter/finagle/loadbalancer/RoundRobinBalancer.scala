package com.twitter.finagle.loadbalancer

import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.{ClientConnection, NoBrokersAvailableException, Service, ServiceFactory, ServiceFactoryProxy, Status}
import com.twitter.util.{Activity, Future, Promise, Time}
import java.util.concurrent.atomic.AtomicLong

/**
 * A simple round robin balancer that chooses the next backend in
 * the list for each request.
 */
class RoundRobinBalancer[Req, Rep](
    val activity: Activity[Traversable[ServiceFactory[Req, Rep]]],
    val statsReceiver: StatsReceiver,
    val emptyException: NoBrokersAvailableException,
    val maxEffort: Int = 5)
  extends ServiceFactory[Req, Rep]
  with Balancer[Req, Rep]
  with Updating[Req, Rep] {

  // For the OnReady mixin
  private[this] val ready = new Promise[Unit]
  override def onReady: Future[Unit] = ready

  protected[this] val maxEffortExhausted: Counter = statsReceiver.counter("max_effort_exhausted")

  protected class Node(val factory: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy[Req,Rep](factory)
    with NodeT[Req,Rep] { self =>
    type This = Node
    // Note: These stats are never updated.
    def load: Double = 0.0
    def pending: Int = 0
    def token: Int = 0

    override def close(deadline: Time): Future[Unit] = factory.close(deadline)
    override def apply(conn: ClientConnection): Future[Service[Req,Rep]] = factory(conn)
  }

  /**
   * A simple round robin distributor.
   */
  protected class Distributor(vector: Vector[Node])
    extends DistributorT[Node](vector) {
    type This = Distributor

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
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty)
  protected def newNode(factory: ServiceFactory[Req,Rep], statsReceiver: StatsReceiver): Node = new Node(factory)
  protected def failingNode(cause: Throwable): Node = new Node(new FailingFactory(cause))
}
