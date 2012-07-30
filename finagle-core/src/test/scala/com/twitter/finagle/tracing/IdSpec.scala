package com.twitter.finagle.tracing

import org.specs.SpecificationWithJUnit
import com.twitter.util.RichU64Long
import scala.util.Random

class IdSpec extends SpecificationWithJUnit {
  "Id" should {
    "not compare" in {
      "unequal ids" in {
        TraceId(None, None, SpanId(0L), None) must be_!=(TraceId(None, None, SpanId(1L), None))
      }
    }

    "compare" in {
      "equal ids" in {
        TraceId(None, None, SpanId(0L), None) must be_==(
          TraceId(None, None, SpanId(0L), None))
      }

      "synthesized parentId" in {
        TraceId(None, Some(SpanId(1L)), SpanId(1L), None) must be_==(
          TraceId(None, None, SpanId(1L), None))
      }

      "synthesized traceId" in {
        TraceId(Some(SpanId(1L)), Some(SpanId(1L)), SpanId(1L), None) must be_==(
          TraceId(None, Some(SpanId(1L)), SpanId(1L), None))
      }
    }

    "return sampled true if debug mode" in {
      TraceId(None, None, SpanId(0L), None, Flags().setDebug).sampled must be_==(Some(true))
    }

    "be correct" in {
      def hex(l: Long) = new RichU64Long(l).toU64HexString

      "each bit" in {
        for (b <- 0 until 64)
          hex(1<<b) must be_==(SpanId(1<<b).toString)
      }

      "random" in {
        val rng = new Random(31415926535897932L)
        for (_ <- 0 until 1024) {
          val l = rng.nextLong()
          hex(l) must be_==(SpanId(l).toString)
        }
      }
    }
  }
}
