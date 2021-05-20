package com.twitter.finagle.server

import org.scalatest.funsuite.AnyFunSuite

class WriteOnceTest extends AnyFunSuite {

  test("write does not allow null") {
    val wo = new WriteOnce[String]("hello")
    intercept[IllegalArgumentException] {
      wo.write(null)
    }
  }

  test("initialize throws on second call") {
    val wo = new WriteOnce[Int](1234)
    wo.write(1235)
    intercept[IllegalStateException] {
      wo.write(1236)
    }
  }

  test("apply returns the uninitialized value before write has been called") {
    val wo = new WriteOnce[Int](1234)
    assert(1234 == wo())
  }

  test("apply returns the initialized value after write has been called") {
    val wo = new WriteOnce[Int](1234)
    assert(1234 == wo())
    wo.write(4321)
    assert(4321 == wo())
  }

}
