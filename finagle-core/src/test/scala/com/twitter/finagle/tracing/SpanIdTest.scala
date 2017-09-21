package com.twitter.finagle.tracing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class SpanIdTest extends FunSuite {
  test("parse positive long") {
    assert(SpanId.fromString("7fffffffffffffff").get.toLong == Long.MaxValue)
  }

  test("parse negative long") {
    assert(SpanId.fromString("8000000000000000").get.toLong == Long.MinValue)
  }

  test("create a span with the ID 123 from hex '7b'") {
    assert(SpanId.fromString("7b").get.toLong == 123L)
  }

  test("create a span from a 128bit hex string without dropping high bits") {
    val hex128Bits = "463ac35c9f6413ad48485a3953bb6124"
    val lower64Bits = "48485a3953bb6124"
    assert(SpanId.fromString(hex128Bits).get != SpanId.fromString(lower64Bits).get)
  }

  test("create a span from a 128bit hex string with properly set low/high bits") {
    val hex128Bits = "463ac35c9f6413ad48485a3953bb6124"
    val span: SpanId = SpanId.fromString(hex128Bits).get
    assert(span.low == 5208512171318403364L)
    assert(span.high == 5060571933882717101L)
  }

  test("drop high bits when calling toLong") {
    assert(SpanId.fromString("463ac35c9f6413ad7fffffffffffffff").get.toLong == Long.MaxValue)
  }

  test("return None if string is not valid hex") {
    assert(SpanId.fromString("rofl") == None)
  }

  test("represent a span with low bits = 123 as the hex '000000000000007b'") {
    assert(SpanId(123L).toString == "000000000000007b") // padded for lexical ordering
  }

  test("represent a 128-bit span with high = Long.MaxValue and low = Long.MinValue as the hex '7fffffffffffffff8000000000000000'") {
    val span: SpanId = SpanId(Long.MaxValue, Long.MinValue)
    assert(span.toString == "7fffffffffffffff8000000000000000")
  }

  test("be equal if the underlying value is equal for 64-bit spans") {
    val a = SpanId(1234L)
    val b = SpanId(1234L)

    assert(a == b)
  }

  test("be equal if the underlying values are equal for 128-bit spans") {
    val a = SpanId(98765L, 43210L)
    val b = SpanId(98765L, 43210L)

    assert(a == b)
  }
}
