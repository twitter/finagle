package com.twitter.finagle.netty3

import org.junit.runner.RunWith

import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NumWorkersTest extends FunSpec {
  describe("numWorkers") {
    it("should have 2 * the number of available processors according to the runtime by default") {
      assert(System.getProperty("com.twitter.jvm.numProcs") == null)
      assert(numWorkers() == Runtime.getRuntime().availableProcessors() * 2)
    }

    it("should be settable as a flag") {
      val old = numWorkers()
      numWorkers.parse("20")

      assert(numWorkers() == 20)

      numWorkers.parse()
      assert(numWorkers() == old)
    }
  }
}
