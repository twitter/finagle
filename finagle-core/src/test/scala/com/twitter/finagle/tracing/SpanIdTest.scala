package com.twitter.finagle.tracing

import org.scalatest.funsuite.AnyFunSuite

class SpanIdTest extends AnyFunSuite {
  test("parse positive long") {
    assert(SpanId.fromString("7fffffffffffffff").get.toLong == Long.MaxValue)
  }

  test("parse negative long") {
    assert(SpanId.fromString("8000000000000000").get.toLong == Long.MinValue)
  }

  test("create a span with the ID 123 from hex '7b'") {
    assert(SpanId.fromString("7b").get.toLong == 123L)
  }

  test("create a span from a 128bit hex string by dropping high bits") {
    val hex128Bits = "463ac35c9f6413ad48485a3953bb6124"
    val lower64Bits = "48485a3953bb6124"
    assert(SpanId.fromString(hex128Bits).get == SpanId.fromString(lower64Bits).get)
  }

  test("return None if string is not valid hex") {
    assert(SpanId.fromString("rofl") == None)
  }

  test("represent a span with the ID 123 as the hex '000000000000007b'") {
    assert(SpanId(123L).toString == "000000000000007b") // padded for lexical ordering
  }

  test("be equal if the underlying value is equal") {
    val a = SpanId(1234L)
    val b = SpanId(1234L)

    assert(a == b)
  }
}
