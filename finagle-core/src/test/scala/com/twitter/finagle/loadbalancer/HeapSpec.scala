package com.twitter.finagle.loadbalancer

import org.specs.SpecificationWithJUnit

import scala.util.Random
import scala.collection.mutable.HashMap

class HeapSpec extends SpecificationWithJUnit {
  "Heap" should {
    val N = 100
    val heap = new Array[Int](N+1)
    val input = (new Random).shuffle(Seq((0 until 100):_*)).toArray
    val indices = new HashMap[Int, Int]
    val indexer = new Heap.Indexer[Int] {
      def apply(v: Int, i: Int) {
        indices(v) = i
      }
    }
    val ops = Heap[Int](math.Ordering.Int, indexer)
    import ops._
    "produce valid heaps" in {
      N to 1 by -1 foreach { i =>
        heap(N+1 - i) = input(N - i)
        fixUp(heap, N+1 - i)
        isValid(heap, 1, N+1 - i) must beTrue
      }
    }

    "provide sorted output (heapsort)" in {
      N to 1 by -1 foreach { i =>
        heap(N+1 - i) = input(N - i)
        fixUp(heap, N+1 - i)
        val copy = heap.clone()
        val ordered = new Array[Int](N+1 - i)
        0 until (N+1 - i) foreach { j =>
          ordered(j) = copy(1)
          copy(1) = copy(N+1 - i - j)
          fixDown(copy, 1, N+1 - i - j)
        }

        ordered.toSeq must be_==((input take(N - i + 1) sorted) toSeq)
      }
    }

    "correctly maintain indices" in {
      N to 1 by -1 foreach { i =>
        heap(N+1 - i) = input(N - i)
        indices(input(N - i)) = N+1 - i
        val fixed = fixUp(heap, N+1 - i)
        1 to N+1 - i foreach { j =>
          indices(heap(j)) must be_==(j)
        }
      }

      1 until N foreach { i =>
        heap(1) = heap(N - i + 1)
        indices(heap(1)) = 1
        fixDown(heap, 1, N - i)
        1 to N-i foreach { j =>
          indices(heap(j)) must be_==(j)
        }
      }
    }
  }
}
