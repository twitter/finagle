package com.twitter.finagle.loadbalancer

import com.twitter.finagle.{ClientConnection, NoBrokersAvailableException, Service,
  ServiceFactory, ServiceFactoryProxy, Status}
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Activity, Future, Promise, Time}

/**
 * A simple sticky load balancer which selects the first backend it
 * can connect to and sticks to it unless that backend is not functioning
 * properly, in which case it will change to another functioning backend.
 */
class StickyBalancer[Req, Rep](
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

  protected[this] val maxEffortExhausted = statsReceiver.counter("max_effort_exhausted")

  override def apply(conn: ClientConnection): Future[Service[Req,Rep]] = {
    dist.pick()(conn)
  }

  protected class Node(val factory: ServiceFactory[Req, Rep])
      extends ServiceFactoryProxy[Req,Rep](factory)
      with NodeT[Req,Rep] { self =>
    type This = Node
    // Note: These stats are never updated.
    def load = 0.0
    def pending = 0
    def token = 0

    override def close(deadline: Time): Future[Unit] = factory.close(deadline)
    override def apply(conn: ClientConnection): Future[Service[Req,Rep]] = factory(conn)
  }

  /**
    * A simple sticky distributor.
    */
  protected class Distributor(val vector: Vector[Node])
      extends DistributorT[Node] {
    type This = Distributor

    private[this] var currentNode = 0

    private[this] val nodeUp: Node => Boolean = {_.isAvailable}

    private[this] val nodes = vector

    /**
      * Will only return Nodes that have Status.Open
      */
    def pick(): Node = {
      if (nodes.isEmpty) failingNode(emptyException)
      else {
        val node = nodes(currentNode)
        node.status match {
          case Status.Open => node
          case _ => synchronized {
            var idx = 0
            var res = -1

            while (idx < nodes.size && res == -1) {
              if (nodes(idx).isAvailable) res = idx
              else idx += 1
            }

            if (res != -1) {
              currentNode = res
              nodes(res)
            } else failingNode(emptyException)
          }
        }
      }
    }

    def needsRebuild: Boolean = false

    def rebuild(): This = new Distributor(vector)
    def rebuild(vector: Vector[Node]): This = new Distributor(vector)
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty)
  protected def newNode(factory: ServiceFactory[Req,Rep], statsReceiver: StatsReceiver): Node = new Node(factory)
  protected def failingNode(cause: Throwable): Node = new Node(new FailingFactory(cause))
}
