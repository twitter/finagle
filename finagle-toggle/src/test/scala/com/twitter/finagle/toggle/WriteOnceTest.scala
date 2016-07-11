package com.twitter.finagle.toggle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteOnceTest extends FunSuite {

  test("write does not allow null") {
    val wo = new WriteOnce[Int](1234)
    intercept[IllegalArgumentException] {
      wo.write(null.asInstanceOf[Int])
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
