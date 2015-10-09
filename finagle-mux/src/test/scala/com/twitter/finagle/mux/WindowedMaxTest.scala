package com.twitter.finagle.mux

import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

@RunWith(classOf[JUnitRunner])
class WindowedMaxTest extends FunSuite with Checkers {
  test("return max") {
    check {
      forAll { ary: Array[Long] =>
        forAll(Gen.posNum[Int]) { window: Int =>
          val w = new WindowedMax(window)
          for (v <- ary) w.add(v)

          val expected =
            if (ary.isEmpty) Long.MinValue
            else if (window > ary.length) ary.max
            else ary.takeRight(window).max

          expected == w.get
        }
      }
    }
  }
}