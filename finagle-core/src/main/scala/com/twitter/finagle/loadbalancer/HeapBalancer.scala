package com.twitter.finagle.loadbalancer

import collection.mutable.HashMap
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{Gauge, StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{ClientConnection, Group, NoBrokersAvailableException,
  Service, ServiceFactory, ServiceProxy}
import com.twitter.util._
import java.net.{InetSocketAddress, SocketAddress}
import util.Random

/**
 * An efficient load balancer that operates on Groups.
 */
class HeapBalancer[Req, Rep](
  group: Group[(SocketAddress, ServiceFactory[Req, Rep])],
  globalStatsReceiver: StatsReceiver = NullStatsReceiver,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  emptyException: NoBrokersAvailableException = new NoBrokersAvailableException
)
  extends ServiceFactory[Req, Rep]
{
  case class Node(
    factory: ServiceFactory[Req, Rep],
    stats: StatsReceiver,
    var load: Int,
    var maxLoad: Int, // (maxLoad <= 0) means unlimited
    var index: Int)

  private[this] val rng = new Random

  private[this] val HeapOps = Heap[Node](
    Ordering.by(_.load),
    new Heap.Indexer[Node] {
      def apply(node: Node, i: Int) {
        node.index = i
      }
    }
  )
  import HeapOps._

  private[this] def createStatsReceiver(addr: SocketAddress) = {
    val address = addr match {
      case inet: InetSocketAddress => inet.getHostName + ":" + inet.getPort
      case socket => socket.toString
    }
    statsReceiver.scope(address)
  }

  // TODO: Remove this as soon as the trait CumulativeGauge has been deleted
  // Hashmap to hold references to the gauges, which are otherwise only weakly referenced
  private[this] val gauges = HashMap.empty[Node, (Gauge, Gauge)]
  private[this] def addGauges(node: Node) = {
    val availableGauge = node.stats.addGauge("available") {
      if (node.factory.isAvailable) 1F else 0F
    }
    val loadGauge = node.stats.addGauge("load") { node.load.toFloat }
    gauges(node) = (availableGauge, loadGauge)
  }
  private[this] def removeGauges(node: Node) = gauges.remove(node)
  private[this] val sizeGauge = globalStatsReceiver.addGauge("size") { size }
  private[this] val adds = globalStatsReceiver.counter("adds")
  private[this] val removes = globalStatsReceiver.counter("removes")

  private[this] var snap = group()
  private[this] var size = snap.size
  private[this] var limited = 0 // Number of nodes with maxLoad set
  // Build initial heap
  // Our heap is 1-indexed. We make heap[0] a dummy node
  // Invariants:
  //   1. heap[i].index == i
  //   2. heap.size == size + 1
  private[this] var heap = {
    val heap = new Array[Node](size + 1)
    val failExc = new Exception("Invalid heap operation on index 0")
    heap(0) = Node(new FailingFactory(failExc), NullStatsReceiver, 0, -1, 0)
    val nodes = (snap.toSeq zipWithIndex) map {
      case ((a, f), i) => Node(f, createStatsReceiver(a), 0, -1, i + 1)
    }
    nodes foreach { addGauges(_) }
    nodes.copyToArray(heap, 1, nodes.size)
    heap
  }

  private[this] def addNode(addr: SocketAddress, serviceFactory: ServiceFactory[Req, Rep], maxLoad: Int) {
    size += 1
    if (maxLoad > 0) {
      limited += 1
    }
    val newNode = Node(serviceFactory, createStatsReceiver(addr), 0, maxLoad, size)
    heap = heap :+ newNode
    fixUp(heap, size)
    addGauges(newNode)
    adds.incr()
  }

  private[this] def remNode(serviceFactory: ServiceFactory[Req, Rep]) {
    val i = heap.indexWhere(n => n.factory eq serviceFactory, 1)
    val node = heap(i)
    swap(heap, i, size)
    fixDown(heap, i, size - 1)
    heap = heap.dropRight(1)
    size -= 1
    removeGauges(node)
    node.index = -1 // sentinel value indicating node is no longer in the heap.
    if (node.maxLoad > 0) {
      limited -= 1
    }
    serviceFactory.close()
    removes.incr()
  }

  private[this] def put(n: Node, success: Boolean) = synchronized {
    n.load -= 1
    if (n.maxLoad > 0) {
      if (success) {
        n.maxLoad += 1
      } else {
        n.maxLoad = if (n.maxLoad > 1) n.maxLoad/2 else 1
      }
    }
    if (n.index < 0) {
      // n has already been removed from the group, therefore do nothing
    } else if (n.load == 0 && size > 1) {
      // since we know that n is now <= any element in the heap, we
      // can do interesting stuff without violating the heap
      // invariant.

      // remove n from the heap.
      val i = n.index
      swap(heap, i, size)
      fixDown(heap, i, size - 1)

      // pick a random index in the shrunk heap, insert n
      val j = rng.nextInt(size -1) + 1
      swap(heap, j, size)
      fixUp(heap, j)

      // expand the heap again
      fixUp(heap, size)
    } else {
      fixUp(heap, n.index)
    }
  }

  private[this] def get(i: Int): Node = {
    val n = heap(i)
    if ((n.factory.isAvailable && (n.maxLoad <= 0 || n.load < n.maxLoad)) || size < i*2) n else {
      if (size == i*2) get(i*2) else {
        val left = get(i*2)
        val right = get(i*2 + 1)
        if (left.load <= right.load && left.factory.isAvailable)
          left
        else
          right
      }
    }
  }

  private[this] class Wrapped(n: Node, underlying: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](underlying)
  {
    override def close(deadline: Time) =
      super.close(deadline) ensure {
        put(n, true)
      }
  }

  private[this] def updateGroup(newSnap: Set[(SocketAddress, ServiceFactory[Req, Rep])]): Unit = synchronized {
    for ((_, n) <- snap &~ newSnap) remNode(n)
    val toBeAdded = newSnap &~ snap
    // make sure slow start nodes won't exceed half of all the nodes,
    // also, make sure a new client won't treat all the servers limited
    if ((limited + toBeAdded.size)*2 > size) {
      for ((a, n) <- newSnap &~ snap) addNode(a, n, -1)
    } else {
      for ((a, n) <- newSnap &~ snap) addNode(a, n, 1)
    }
    snap = newSnap
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val node = synchronized {
      val curSnap = group()
      if (curSnap ne snap)
        updateGroup(curSnap)
      if (size == 0)
        return Future.exception(emptyException)
      val n = get(1)
      n.load += 1
      fixDown(heap, n.index, size)
      if (n.maxLoad > 0 && ((n.index > 1 && heap(n.index/2).maxLoad <= 0)
                            || n.index*2 > size)) {
        // either having more loads than a normal node or reaching the bottom of the heap,
        // it will graduate from limited state
        n.maxLoad = -1
        limited -= 1
      }
      n
    }

    node.factory(conn) map { new Wrapped(node, _) } onFailure { _ => put(node, false) }
  }

  def close(deadline: Time) =
    Closable.all(heap.map(_.factory):_*).close(deadline)

  override def isAvailable = true
  override val toString = synchronized("HeapBalancer(%d)".format(size))
}
