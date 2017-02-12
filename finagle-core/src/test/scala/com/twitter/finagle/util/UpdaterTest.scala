package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import java.util.concurrent.{CyclicBarrier, CountDownLatch}

@RunWith(classOf[JUnitRunner])
class UpdaterTest extends FunSuite {
  test("Prioritization") {
    case class Work(p: Int)
    @volatile var worked: Seq[Work] = Nil
    val barrier = new CyclicBarrier(2)
    val first = new CountDownLatch(1)

    val u = new Updater[Work] {
      protected def preprocess(elems: Seq[Work]) =
        Seq(elems.minBy(_.p))

      def handle(w: Work) {
        worked :+= w
        first.countDown()
        barrier.await()
        ()
      }
    }

    val w0 = Work(0)
    val thr = new Thread("Test-Updater") {
      override def run() {
        u(w0)
      }
    }

    thr.start()
    first.await()
    assert(worked == Seq(Work(0)))

    u(Work(3))
    u(Work(10))
    u(Work(1))
    u(Work(3))
    barrier.await()
    barrier.await()
    thr.join()
    assert(worked == Seq(Work(0), Work(1)))
  }
}
