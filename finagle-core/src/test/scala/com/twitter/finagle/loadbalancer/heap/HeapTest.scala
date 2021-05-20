package com.twitter.finagle.loadbalancer.heap

import scala.util.Random
import scala.collection.mutable.HashMap
import org.scalatest.funsuite.AnyFunSuite

class HeapTest extends AnyFunSuite {
  private class Helper {
    val N = 100
    val heap = new Array[Int](N + 1)
    val input = (new Random).shuffle(Seq(0 until 100: _*)).toArray
    val indices = new HashMap[Int, Int]
    val indexer = new Heap.Indexer[Int] {
      def apply(v: Int, i: Int): Unit = {
        indices(v) = i
      }
    }
    val ops = Heap[Int](math.Ordering.Int, indexer)
  }

  test("heap should produce valid heaps") {
    val h = new Helper
    import h._
    import h.ops._

    N to 1 by -1 foreach { i =>
      heap(N + 1 - i) = input(N - i)
      fixUp(heap, N + 1 - i)
      assert(isValid(heap, 1, N + 1 - i))
    }
  }

  test("provide sorted output (heapsort)") {
    val h = new Helper
    import h._
    import h.ops._

    N to 1 by -1 foreach { i =>
      heap(N + 1 - i) = input(N - i)
      fixUp(heap, N + 1 - i)
      val copy = heap.clone()
      val ordered = new Array[Int](N + 1 - i)
      0 until (N + 1 - i) foreach { j =>
        ordered(j) = copy(1)
        copy(1) = copy(N + 1 - i - j)
        fixDown(copy, 1, N + 1 - i - j)
      }

      assert(ordered.toSeq === input.take(N - i + 1).sorted)
    }
  }

  test("correctly maintain indices") {
    val h = new Helper
    import h._
    import h.ops._

    N to 1 by -1 foreach { i =>
      heap(N + 1 - i) = input(N - i)
      indices(input(N - i)) = N + 1 - i
      val fixed = fixUp(heap, N + 1 - i)
      1 to N + 1 - i foreach { j => assert(indices(heap(j)) == j) }
    }

    1 until N foreach { i =>
      heap(1) = heap(N - i + 1)
      indices(heap(1)) = 1
      fixDown(heap, 1, N - i)
      1 to N - i foreach { j => assert(indices(heap(j)) == j) }
    }
  }
}
