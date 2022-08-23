package com.twitter.finagle.exp.fiber_scheduler.util

import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicIntegerArray

class XSRandomTest extends FiberSchedulerSpec {

  val max = 50
  val iterations = 100000

  "distributes the generated values" - {
    "single thread" in {
      val counts = new Array[Int](max)
      for (_ <- 0 until iterations) {
        counts(XSRandom.nextInt(max)) += 1
      }
      check(counts)
    }
    "multiple threads" in {
      val threads = 10
      val atomicCounts = new AtomicIntegerArray(max)
      val exec = Executors.newCachedThreadPool()
      val cdl = CountDownLatch(threads)
      for (_ <- 0 until threads) {
        exec.execute(() => {
          for (_ <- 0 until iterations) {
            atomicCounts.incrementAndGet(XSRandom.nextInt(max))
          }
          cdl.countDown()
        })
      }
      cdl.await()
      val counts = new Array[Int](max)
      for (i <- 0 until max) {
        counts(i) = atomicCounts.get(i)
      }
      check(counts)
    }
  }

  private[this] def check(counts: Array[Int]) =
    assert((counts.max - counts.min).toDouble / (counts.sum / max) < 0.3)

}
