package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalactic.{TripleEquals, Tolerance}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EmaTest extends FunSuite {
  import TripleEquals._
  import Tolerance._

  test("updates are time invariant") {
    val a, b = new Ema(1000)
    assert(a.update(10, 10) == 10)
    assert(a.update(20, 10) == 10)
    assert(a.update(30, 10) == 10)

    assert(b.update(10, 10) == 10)
    assert(b.update(30, 10) == 10)

    assert(a.update(40, 5) == b.update(40, 5))

    assert(a.update(50, 5) > a.update(60, 5))

    assert(a.update(60, 5) === b.update(60, 5) +- 0.0001)
  }

  test("No averaging when the window=0") {
    val e = new Ema(0)

    assert(e.update(1, 10) == 10)
    assert(e.update(2, 20) == 20)
    assert(e.update(3, 30) == 30)
    assert(e.update(4, 0) == 0)
  }

  test("ema and time are reset on reset()") {
    val e = new Ema(5)
    assert(e.update(1, 3) == 3)

    e.reset()

    assert(e.last == 0D)
    assert(e.update(2, 5) == 5)
  }
}
