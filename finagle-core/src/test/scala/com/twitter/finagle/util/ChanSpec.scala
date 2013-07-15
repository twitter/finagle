package com.twitter.finagle.util

import org.specs.SpecificationWithJUnit
import java.util.concurrent.CyclicBarrier
import scala.collection.mutable.Buffer
import java.util.concurrent.CountDownLatch

class ChanSpec extends SpecificationWithJUnit {
  "Proc" should {
    "admit one at a time, in the order received, queueing items" in {
      val threads = Buffer[Thread]()
      val l = new CountDownLatch(1)
      val b = new CyclicBarrier(2)

      val p = Proc[Thread] { t => threads += t; l.countDown(); b.await() }

      val t0 = new Thread {
        override def run() {
          p ! this
        }
      }

      val t1 = new Thread {
        override def run() {
          l.await()
          p ! this
          b.await()
          b.await()
        }
      }

      t0.start(); t1.start()
      t0.join(); t1.join()

      threads.toSeq mustEqual Seq(t0, t1)
    }
  }
}
