package com.twitter.finagle.loadbalancer

import util.Random
import com.twitter.finagle.{Service, ServiceProxy, ServiceFactory, NoBrokersAvailableException, ClientConnection}
import com.twitter.finagle.stats.{Gauge, StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.builder.Cluster
import collection.mutable.HashMap
import com.twitter.util._
import com.twitter.finagle.service.FailingFactory

/**
 * An efficient load balancer that operates on Clusters.
 */
class HeapBalancer[Req, Rep](
  cluster: Cluster[ServiceFactory[Req, Rep]],
  statsReceiver: StatsReceiver = NullStatsReceiver,
  exception: NoBrokersAvailableException = new NoBrokersAvailableException
)
  extends ServiceFactory[Req, Rep]
{
  case class Node(
    factory: ServiceFactory[Req, Rep],
    var load: Int,
    var index: Int)

  private[this] val rng = new Random


  private[this] val HeapOps = Heap[Node](
    Ordering.by { _.load },
    new Heap.Indexer[Node] {
      def apply(node: Node, i: Int) {
        node.index = i
      }
    }
  )
  import HeapOps._

  // Hashmap to hold references to the gauges, which are otherwise only weakly referenced
  private[this] val gauges = HashMap.empty[Node, (Gauge, Gauge)]
  private[this] def addGauges(node: Node) = {
    val availableGauge = statsReceiver.scope("available").addGauge(node.factory.toString) {
      if (node.factory.isAvailable) 1F else 0F
    }
    val loadGauge = statsReceiver.scope("load").addGauge(node.factory.toString) {
      node.load.toFloat
    }
    gauges(node) = (availableGauge, loadGauge)
  }
  private[this] def removeGauges(node: Node) = gauges.remove(node)
  private[this] val sizeGauge = statsReceiver.addGauge("size") { size }
  private[this] val adds = statsReceiver.counter("adds")
  private[this] val removes = statsReceiver.counter("removes")

  private[this] var (factories, updates) = cluster.snap
  // Build initial heap
  // Our heap is 1-indexed. We make heap[0] a dummy node
  // Invariants:
  //   1. heap[i].index == i
  //   2. heap.size == size + 1
  @volatile private[this] var size = factories.size
  private[this] var heap = {
    val heap = new Array[Node] (size + 1)
    heap(0) = new Node(new FailingFactory(new Exception("Invalid heap operation on index 0")), 0, 0)
    val nodes = (factories zipWithIndex) map { case (f, i) => Node(f, 0, i + 1) }
    nodes foreach { addGauges(_) }
    nodes.copyToArray(heap, 1, nodes.size)
    heap
  }

  updates foreach { spool =>
    spool foreach {
      case Cluster.Add(elem) => synchronized {
        size += 1
        val newNode = Node(elem, 0, size)
        heap = heap :+ newNode
        fixUp(heap, size)
        addGauges(newNode)
        adds.incr()
      }
      case Cluster.Rem(elem) => synchronized {
        val i = heap.indexWhere(n => n.factory eq elem, 1)
        val node = heap(i)
        swap(heap, i, size)
        fixDown(heap, i, size - 1)
        heap = heap.dropRight(1)
        size -= 1
        removeGauges(node)
        node.index = -1 // sentinel value indicating node is no longer in the heap.
        elem.close()
        removes.incr()
      }
    }
  }
  // Zero out updates so that we don't hold
  // on to the head of the spool; factories so
  // we don't hold on to the initial list.
  updates = null
  factories = null

  private[this] def put(n: Node) = synchronized {
    n.load -= 1
    if (n.index < 0) {
      // n has already been removed from the cluster, therefore do nothing
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
    if (n.factory.isAvailable || size < i*2) n else {
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
    override def release() {
      super.release()
      put(n)
    }
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (size == 0) return Future.exception(exception)

    val n = synchronized {
      val n = get(1)
      n.load += 1
      fixDown(heap, n.index, size)
      n
    }
    n.factory(conn) map { new Wrapped(n, _) } onFailure { _ => put(n) }
  }

  def close() {
    heap foreach { _.factory.close() }
  }

  override def isAvailable = true
  override val toString = "HeapBalancer(%d)".format(size)
}
