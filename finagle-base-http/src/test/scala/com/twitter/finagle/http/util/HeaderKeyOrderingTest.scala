package com.twitter.finagle.http.util

import org.scalatest.funsuite.AnyFunSuite

class HeaderKeyOrderingTest extends AnyFunSuite {
  test("keys are case insensitive") {
    val key1 = "coffee"
    val key2 = "COFFEE"
    val key3 = "coFfEe"

    // case difference ends up equal
    assert(HeaderKeyOrdering.compare(key1, key2) == 0)
    assert(HeaderKeyOrdering.compare(key2, key3) == 0)
    assert(HeaderKeyOrdering.compare(key1, key3) == 0)
  }

  test("different keys are not equal") {
    val key1 = "coffee"
    val key2 = "covfefe"
    assert(HeaderKeyOrdering.compare(key1, key2) != 0)
  }
}
