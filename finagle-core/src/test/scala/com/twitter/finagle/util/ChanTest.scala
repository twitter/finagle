package com.twitter.finagle.util

import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import scala.collection.mutable.Buffer
import org.scalatest.funsuite.AnyFunSuite

class ChanTest extends AnyFunSuite {
  test("Proc should admit one at a time, in the order received, queueing items") {
    val threads = Buffer[Thread]()
    val l = new CountDownLatch(1)
    val b = new CyclicBarrier(2)

    val p = Proc[Thread] { t => threads += t; l.countDown(); b.await() }

    val t0 = new Thread {
      override def run(): Unit = {
        p ! this
      }
    }

    val t1 = new Thread {
      override def run(): Unit = {
        l.await()
        p ! this
        b.await()
        b.await()
      }
    }

    t0.start()
    t1.start()
    t0.join()
    t1.join()

    assert(threads.toSeq == Seq(t0, t1))
  }

  test("Proc should swallow exceptions") {
    val p = Proc[Int] { _ => throw new RuntimeException }
    assert((p ! 4) === ((): Unit))
  }

}
