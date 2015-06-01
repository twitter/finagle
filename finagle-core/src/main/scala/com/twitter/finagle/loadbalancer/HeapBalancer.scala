package com.twitter.finagle.loadbalancer

import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.OnReady
import com.twitter.finagle.{
  ClientConnection, Group, NoBrokersAvailableException, Service, ServiceFactory,
  ServiceProxy, Status}
import com.twitter.util._
import scala.annotation.tailrec
import scala.util.Random

object HeapBalancer {
  val Penalty = Int.MaxValue
  val Zero = Int.MinValue + 1
}

/**
 * An efficient load balancer that operates on Activity[Set[ServiceFactory[Req, Rep]]].
 */
class HeapBalancer[Req, Rep](
  factories: Activity[Set[ServiceFactory[Req, Rep]]],
  statsReceiver: StatsReceiver,
  emptyException: Throwable,
  rng: Random
) extends ServiceFactory[Req, Rep] with OnReady {

  import HeapBalancer._

  private[this] val sizeGauge = statsReceiver.addGauge("size") { synchronized { size } }
  private[this] val adds = statsReceiver.counter("adds")
  private[this] val removes = statsReceiver.counter("removes")

  // Every underlying ServiceFactory is represented in the
  // heap by a Node. If a node's index is < 0, it is discarded.
  // Load begins at Int.MinValue; loads greater than or equal
  // to 0 represent downed nodes. This allows us to maintain
  // a single heap and still load balance in downed scenarios.
  case class Node(
    factory: ServiceFactory[Req, Rep],
    var load: Int,
    var index: Int,
    var downq: Node = null
  )

  // Linked list of downed nodes.
  private[this] var downq: Node = null

  private[this] val HeapOps = Heap[Node](
    Ordering.by(_.load),
    new Heap.Indexer[Node] {
      def apply(node: Node, i: Int) {
        node.index = i
      }
    }
  )
  import HeapOps._

  // Build an initial, empty heap.
  // Our heap is 1-indexed. We make heap[0] a dummy node
  // Invariants:
  //   1. heap[i].index == i
  //   2. heap.size == size + 1
  private[this] var size = 0
  private[this] var heap = {
    val heap = new Array[Node](1)
    val failExc = new Exception("Invalid heap operation on index 0")
    heap(0) = Node(new FailingFactory(failExc), Zero, 0)
    heap
  }
  private[this] var snap = Set[ServiceFactory[Req, Rep]]()

  private[this] val ready = new Promise[Unit]
  def onReady: Future[Unit] = ready

  private[this] val observation = factories.run.changes respond {
    case Activity.Pending =>

    case Activity.Ok(newSet) =>
      updateGroup(newSet)
      ready.setDone()

    case Activity.Failed(_) =>
      // On resolution failure, consider the load balancer ready (to serve errors).
      ready.setDone()
  }

  private[this] val availableGauge = statsReceiver.addGauge("available") {
    val nodes = synchronized { heap.drop(1) }
    nodes.count(_.factory.status == Status.Open)
  }

  private[this] val loadGauge = statsReceiver.addGauge("load") {
    val loads = synchronized {
      heap drop(1) map { n =>
        if (n.load < 0) n.load+Penalty
        else n.load
      }
    }

    loads.sum
  }

  private[this] def addNode(serviceFactory: ServiceFactory[Req, Rep]) {
    size += 1
    val newNode = Node(serviceFactory, Zero, size)
    heap = heap :+ newNode
    fixUp(heap, size)
    adds.incr()
  }

  private[this] def remNode(serviceFactory: ServiceFactory[Req, Rep]) {
    val i = heap.indexWhere(n => n.factory eq serviceFactory, 1)
    val node = heap(i)
    swap(heap, i, size)
    fixDown(heap, i, size - 1)
    heap = heap.dropRight(1)
    size -= 1
    node.index = -1 // sentinel value indicating node is no longer in the heap.
    serviceFactory.close()
    removes.incr()
  }

  private[this] def put(n: Node) = synchronized {
    n.load -= 1
    if (n.index < 0) {
      // n has already been removed from the group, therefore do nothing
    } else if (n.load == Zero && size > 1) {
      // since we know that n is now <= any element in the heap, we
      // can do interesting stuff without violating the heap
      // invariant.

      // remove n from the heap.
      val i = n.index
      swap(heap, i, size)
      fixDown(heap, i, size - 1)

      // pick a random node with which we can swap n
      val j = rng.nextInt(size) + 1
      swap(heap, j, size)
      fixUp(heap, j)

      // expand the heap again
      fixUp(heap, size)
    } else {
      fixUp(heap, n.index)
    }
  }

  @tailrec
  private[this] def get(): Node = {
    var n = downq
    var m = null: Node
    while (n != null) {
      if (n.index < 0) {  // discarded node
        n = n.downq
        if (m == null) downq = n
        else m.downq = n
      } else if (n.factory.status == Status.Open) {  // revived node
        n.load -= Penalty
        fixUp(heap, n.index)
        val o = n.downq
        n.downq = null
        n = o
        if (m == null) downq = n
        else m.downq = n
      } else {  // unchanged
        m = n
        n = n.downq
      }
    }

    n = heap(1)
    if (n.factory.status == Status.Open || n.load >= 0) n else {
      // Mark as down.
      n.downq = downq
      downq = n
      n.load += Penalty
      fixDown(heap, 1, size)
      get()
    }
  }

  private[this] class Wrapped(n: Node, underlying: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](underlying)
  {
    override def close(deadline: Time) =
      super.close(deadline) ensure {
        put(n)
      }
  }

  private[this] def updateGroup(newSnap: Set[ServiceFactory[Req, Rep]]): Unit =
    synchronized {
      for (n <- snap &~ newSnap) remNode(n)
      for (n <- newSnap &~ snap) addNode(n)
      snap = newSnap
    }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val node = synchronized {
      if (size == 0)
        return Future.exception(emptyException)
      val n = get()
      n.load += 1
      fixDown(heap, n.index, size)
      n
    }

    node.factory(conn) map { new Wrapped(node, _) } onFailure { _ => put(node) }
  }

  private[this] val nodesClosable: Closable = Closable.make { deadline =>
    Closable.all(synchronized(heap).map(_.factory):_*).close(deadline)
  }

  def close(deadline: Time) = {
    Closable.sequence(observation, nodesClosable).close(deadline)
  }

  /**
   * HeapBalancer status is the best of its constituent nodes, excluding
   * the heap(0) node because our implementation is 1-indexed.
   */
  override def status: Status = Status.bestOf(heap.drop(1), nodeStatus)

  private[this] val nodeStatus: Node => Status = _.factory.status

  override val toString = synchronized("HeapBalancer(%d)".format(size))
}
