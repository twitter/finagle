package com.twitter.finagle.tracing

import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

class IdTest extends AnyFunSuite {
  test("compare unequal ids") {
    assert(TraceId(None, None, SpanId(0L), None) != TraceId(None, None, SpanId(1L), None))
  }

  test("compare unequal ids (128bit)") {
    val first = TraceId(Some(SpanId(1L)), None, SpanId(2L), None, Flags(), Some(SpanId(3L)))
    val second = TraceId(Some(SpanId(1L)), None, SpanId(2L), None, Flags(), Some(SpanId(4L)))
    assert(first != second)
  }

  test("compare equal ids") {
    assert(TraceId(None, None, SpanId(0L), None) == TraceId(None, None, SpanId(0L), None))
  }

  test("compare equal ids (128bit)") {
    val first = TraceId(Some(SpanId(1L)), None, SpanId(2L), None, Flags(), Some(SpanId(3L)))
    val second = TraceId(Some(SpanId(1L)), None, SpanId(2L), None, Flags(), Some(SpanId(3L)))
    assert(first == second)
  }

  test("compare synthesized parentId") {
    assert(
      TraceId(None, Some(SpanId(1L)), SpanId(1L), None) ==
        TraceId(None, None, SpanId(1L), None)
    )
  }

  test("compare synthesized traceId") {
    assert(
      TraceId(Some(SpanId(1L)), Some(SpanId(1L)), SpanId(1L), None) ==
        TraceId(None, Some(SpanId(1L)), SpanId(1L), None)
    )
  }

  test("serialize and deserialize") {
    val traceIdOne = TraceId(None, Some(SpanId(1L)), SpanId(1L), None)
    assert(traceIdOne == TraceId.deserialize(TraceId.serialize(traceIdOne)).get())
    val traceIdTwo = TraceId(None, None, SpanId(0L), None, Flags().setDebug)
    assert(traceIdTwo == TraceId.deserialize(TraceId.serialize(traceIdTwo)).get())
  }

  test("fail to deserialize incorrect traces") {
    val badTrace = "not-a-trace".getBytes()
    assert(TraceId.deserialize(badTrace).isThrow)
  }

  test("return sampled true if debug mode") {
    assert(TraceId(None, None, SpanId(0L), None, Flags().setDebug).sampled == Some(true))
  }

  def hex(l: Long): String = "%016X".format(l)

  test("extract 64bit only ids") {
    val low = 5208512171318403364L
    val spanId = hex(low)
    val traceId = TraceId128(spanId)

    assert(traceId.high.isDefined == false)
    assert(traceId.low.isDefined)
    assert(traceId.low.get.self == low)
  }

  test("extract 128bit ids") {
    val low = 5208512171318403364L
    val high = 5060571933882717101L
    val spanId = hex(high) + hex(low)
    val traceId = TraceId128(spanId)

    assert(traceId.high.isDefined)
    assert(traceId.high.get.self == high)
    assert(traceId.low.isDefined)
    assert(traceId.low.get.self == low)
  }

  test("extract to TraceId128Bit.empty when span id is invalid") {
    assert(TraceId128("invalid") == TraceId128.empty)
  }

  test("SpanId.toString: each bit must be correct") {
    for (b <- 0 until 64)
      assert(hex(1 << b).equalsIgnoreCase(SpanId(1 << b).toString))
  }

  test("SpanId.toString: random") {
    val rng = new Random(31415926535897932L)
    for (_ <- 0 until 1024) {
      val l = rng.nextLong()
      assert(hex(l).equalsIgnoreCase(SpanId(l).toString))
    }
  }

  test("hashCode only accounts for id fields") {
    assert(
      TraceId(
        Some(SpanId(1L)),
        Some(SpanId(2L)),
        SpanId(3L),
        Some(true),
        Flags(),
        Some(SpanId(4L))).hashCode ==
        TraceId(
          Some(SpanId(1L)),
          Some(SpanId(2L)),
          SpanId(3L),
          Some(false),
          Flags(Flags.Debug),
          Some(SpanId(4L))
        ).hashCode
    )
  }
}
