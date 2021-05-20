package com.twitter.finagle.toggle

import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class NullToggleMapTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  test("apply") {
    val toggle = NullToggleMap("hi")
    assert(toggle.isUndefined)
  }

  test("iterator") {
    assert(NullToggleMap.iterator.isEmpty)
  }

}
