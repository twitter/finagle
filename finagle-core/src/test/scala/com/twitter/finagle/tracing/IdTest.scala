package com.twitter.finagle.tracing

import com.twitter.util.RichU64Long
import scala.util.Random
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IdTest extends FunSuite {
  test("compare unequal ids") {
    assert(TraceId(None, None, SpanId(0L), None) != TraceId(None, None, SpanId(1L), None))
  }

  test("compare equal ids") {
    assert(TraceId(None, None, SpanId(0L), None) === TraceId(None, None, SpanId(0L), None))
  }

  test("compare synthesized parentId") {
    assert(TraceId(None, Some(SpanId(1L)), SpanId(1L), None) ===
      TraceId(None, None, SpanId(1L), None))
  }

  test("compare synthesized traceId") {
    assert(TraceId(Some(SpanId(1L)), Some(SpanId(1L)), SpanId(1L), None) ===
      TraceId(None, Some(SpanId(1L)), SpanId(1L), None))
  }

  test("return sampled true if debug mode") {
    assert(TraceId(None, None, SpanId(0L), None, Flags().setDebug).sampled === Some(true))
  }

  def hex(l: Long) = new RichU64Long(l).toU64HexString

  test("SpanId.toString: each bit must be correct") {
    for (b <- 0 until 64)
      assert(hex(1<<b) === SpanId(1<<b).toString)
  }

  test("SpanId.toString: random") {
    val rng = new Random(31415926535897932L)
    for (_ <- 0 until 1024) {
      val l = rng.nextLong()
      assert(hex(l) === SpanId(l).toString)
    }
  }
}
