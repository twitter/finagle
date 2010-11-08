package com.twitter.finagle.util

import java.util.concurrent.{CountDownLatch, CyclicBarrier}

import org.specs.Specification

object SerializedSpec extends Specification with Serialized {
  "Serialized" should {
    "runs blocks, one at a time, in the order received" in {
      val t1CallsSerializedFirst = new CountDownLatch(1)
      val t1FinishesWork = new CountDownLatch(1)
      val orderOfExecution = new collection.mutable.ListBuffer[Thread]

      val t1 = new Thread {
        override def run {
          serialized {
            t1CallsSerializedFirst.countDown()
            t1FinishesWork.await()
            orderOfExecution += this
            ()
          }
        }
      }

      val t2 = new Thread {
        override def run {
          t1CallsSerializedFirst.await()
          serialized {
            orderOfExecution += this
            ()
          }
          t1FinishesWork.countDown()
        }
      }

      t1.start()
      t2.start()
      t1.join()
      t2.join()

      orderOfExecution.toList mustEqual List(t1, t2)
    }
  }
}
