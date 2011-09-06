package com.twitter.finagle.loadbalancer

import scala.util.Random
import scala.collection.mutable.HashMap

import com.twitter.util.Future

import com.twitter.finagle.{
  Service, ServiceProxy, ServiceFactory,
  NoBrokersAvailableException}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}

/**
 * An efficient heap-based load balancer.
 *
 * It has good shuffling, but it's not perfect.  It currently assumes
 * that the input factory sequence is static.
 */
class HeapBalancer[Req, Rep](
  factories: Seq[ServiceFactory[Req, Rep]],
  statsReceiver: StatsReceiver = NullStatsReceiver)
  extends ServiceFactory[Req, Rep]
{
  // todo: handle nonavailable nodes somewhat
  // better. maybe a separate list?
  private[this] val rng = new Random
  private[this] val size = factories.size

  private[this] case class Node(
    factory: ServiceFactory[Req, Rep],
    var load: Int,
    var index: Int)
  private[this] val HeapOps = Heap[Node](
    Ordering.by { _.load },
    new Heap.Indexer[Node] {
      def apply(node: Node, i: Int) {
        node.index = i
      }
    }
  )
  import HeapOps._

  private[this] val heap = {
    // populate the heap.  since the nodes all
    // start out unloaded, we form a valid heap by
    // simply copying them to an array.  our heap
    // is 1-indexed for easier arithmetic.

    val heap = new Array[Node](size + 1)
    val nodes = (factories zipWithIndex) map { case (f, i) => Node(f, 0, i + 1) }
    nodes.copyToArray(heap, 1, nodes.size)
    heap
  }

  private[this] val gauges = heap drop 1 map { n =>
    statsReceiver.scope("available").addGauge(n.factory.toString) {
      if (n.factory.isAvailable) 1F else 0F
    }

    statsReceiver.scope("load").addGauge(n.factory.toString) {
      n.load.toFloat
    }
  }

  private[this] def put(n: Node) = synchronized {
    n.load -= 1
    if (n.load == 0 && size > 1) {
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

  def make(): Future[Service[Req, Rep]] = {
    if (size == 0) return Future.exception(new NoBrokersAvailableException)
  
    val n = synchronized {
      val n = get(1)
      n.load += 1
      fixDown(heap, n.index, size)
      n
    }

    n.factory.make() map { new Wrapped(n, _) } onFailure { _ => put(n) }
  }

  def close() {
    factories foreach { _.close() }
  }

  override def isAvailable = true
  override val toString = "HeapBalancer(%d)".format(factories.size)
}
