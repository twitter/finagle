package com.twitter.finagle.toggle

import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.FunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class NullToggleMapTest extends FunSuite with ScalaCheckDrivenPropertyChecks {

  private val IntGen = arbitrary[Int]

  test("apply") {
    val toggle = NullToggleMap("hi")
    forAll(IntGen) { i =>
      assert(!toggle.isDefinedAt(i))
    }
  }

  test("iterator") {
    assert(NullToggleMap.iterator.isEmpty)
  }

}
