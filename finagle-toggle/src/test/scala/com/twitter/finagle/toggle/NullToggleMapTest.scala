package com.twitter.finagle.toggle

import org.scalatest.FunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class NullToggleMapTest extends FunSuite with ScalaCheckDrivenPropertyChecks {

  test("apply") {
    val toggle = NullToggleMap("hi")
    assert(toggle.isUndefined)
  }

  test("iterator") {
    assert(NullToggleMap.iterator.isEmpty)
  }

}
