package com.twitter.finagle.tracing

import org.specs.Specification

object IdSpec extends Specification {
  "Id" should {
    "not compare" in {
      "unequal ids" in {
        TraceId(None, None, SpanId(0L), false) must be_!=(TraceId(None, None, SpanId(1L), false))
      }
   }

   "compare" in {
     "equal ids" in {
       TraceId(None, None, SpanId(0L), false) must be_==(
         TraceId(None, None, SpanId(0L), false))
     }

     "synthesized parentId" in {
       TraceId(None, Some(SpanId(1L)), SpanId(1L), false) must be_==(
         TraceId(None, None, SpanId(1L), false))
     }

     "synthesized traceId" in {
       TraceId(Some(SpanId(1L)), Some(SpanId(1L)), SpanId(1L), false) must be_==(
         TraceId(None, Some(SpanId(1L)), SpanId(1L), false))
     }
   }
  }
}
