package com.twitter.finagle.loadbalancer.heap

import scala.annotation.tailrec

private object Heap {
  trait Indexer[-T] {
    def apply(t: T, i: Int): Unit
  }

  object Indexer {
    object nil extends Indexer[Any] {
      def apply(t: Any, i: Int): Unit = {}
    }
  }

  def apply[T](ord: Ordering[T], indexer: Indexer[T] = Indexer.nil): Heap[T] =
    new Heap(ord, indexer)
}

/**
 * Provide heap operations for some type T.
 *
 * nb: 1-indexed heaps, ranges are inclusive.
 * indexer will only be invoked on swaps. the
 * caller is responsible for maintaining the
 * initial value.
 */
private class Heap[T](ord: Ordering[T], indexer: Heap.Indexer[T]) {
  import ord._

  def swap(heap: Array[T], i: Int, j: Int): Unit = {
    val tmp = heap(i)
    heap(i) = heap(j)
    heap(j) = tmp
    indexer(heap(i), i)
    indexer(heap(j), j)
  }

  @tailrec
  final def fixDown(heap: Array[T], i: Int, j: Int): Unit = {
    if (j < i * 2) return

    val m = if (j == i * 2 || heap(2 * i) < heap(2 * i + 1)) 2 * i else 2 * i + 1
    if (heap(m) < heap(i)) {
      swap(heap, i, m)
      fixDown(heap, m, j)
    }
  }

  @tailrec
  final def fixUp(heap: Array[T], i: Int): Unit = {
    if (i != 1 && heap(i) < heap(i / 2)) {
      swap(heap, i, i / 2)
      fixUp(heap, i / 2)
    }
  }

  def isValid(heap: Array[T], i: Int, j: Int): Boolean =
    if (j < i * 2) true
    else {
      val left = heap(i) < heap(i * 2)
      val right = if (j == i * 2) true else heap(i) < heap(i * 2 + 1)
      left && right
    }
}
