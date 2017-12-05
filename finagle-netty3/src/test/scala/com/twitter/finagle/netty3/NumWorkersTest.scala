package com.twitter.finagle.netty3

import org.scalatest.FunSuite

class NumWorkersTest extends FunSuite {
  test("numWorkers should be settable as a flag") {
    val old = numWorkers()
    numWorkers.parse("20")

    assert(numWorkers() == 20)

    numWorkers.parse()
    assert(numWorkers() == old)
  }
}
