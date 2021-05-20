package com.twitter.finagle.tracing.opencensus

import io.opencensus.trace.{SpanContext, SpanId, TraceId, TraceOptions, Tracestate}
import java.util.concurrent.ThreadLocalRandom
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class TraceContextFilterTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  private def genSpanContext: Gen[SpanContext] =
    Gen.resultOf { isSampled: Boolean =>
      SpanContext.create(
        TraceId.generateRandomId(ThreadLocalRandom.current),
        SpanId.generateRandomId(ThreadLocalRandom.current),
        TraceOptions.builder().setIsSampled(isSampled).build(),
        Tracestate.builder().build()
      )
    }

  test("serialize / deserialize round trips") {
    forAll(genSpanContext) { ctx: SpanContext =>
      val ser = TraceContextFilter.SpanContextKey.marshal(ctx)
      val deser = TraceContextFilter.SpanContextKey.tryUnmarshal(ser).get()
      assert(deser == ctx)
    }
  }

}
