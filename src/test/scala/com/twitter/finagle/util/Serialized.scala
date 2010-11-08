package com.twitter.finagle.util

import java.util.concurrent.atomic.AtomicInteger

import org.specs.Specification

object SerializedSpec extends Specification with Serialized {
  "Serialized" should {
    "serialize computations" in {
      @volatile var count = 0

      def normalIncr {
        count += 1
      }

      def serialIncr {
        serialized { normalIncr() }
      }

      def alive(ts: Seq[Thread]): Boolean = ts.count(_.isAlive) > 0
      def waitFor(f: Function0[Boolean]) {
        while (f()) Thread.`yield`
      }

      def performManyParallel(incrFn: => Unit): Boolean = {
        for (i <- 1 to 100) {
          count = 0
          var started = new AtomicInteger(0)
          val mutators = for (i <- 1 to 100) yield new Thread {
            override def run {
              started.getAndIncrement()
              incrFn()
            }
          }
          mutators.foreach(_.start)
          while (started.get < 100 && mutators.count(_.isAlive) > 0) Thread.sleep(10)
          if (count != 100) return false
        }

        true
      }

      "computations are unordered sans serializer" in {
        performManyParallel(normalIncr) must beFalse
      }

      "computations are ordered with serializer" in {
        performManyParallel(serialIncr) must beTrue
      }
    }
  }
}
