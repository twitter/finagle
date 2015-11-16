package com.twitter.finagle.util

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.util.concurrent.CyclicBarrier
import scala.collection.mutable.Buffer
import java.util.concurrent.CountDownLatch

@RunWith(classOf[JUnitRunner])
class ChanTest extends FunSuite {
  test("Proc should admit one at a time, in the order received, queueing items") {
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

    t0.start();
    t1.start()
    t0.join();
    t1.join()

    assert(threads.toSeq == Seq(t0, t1))
  }

  test("Proc should swallow exceptions") {
    val p = Proc[Int] { _ => throw new RuntimeException }
    assert((p ! 4) ===())
  }

}
