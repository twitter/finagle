package com.twitter.finagle.tracing

import org.specs.SpecificationWithJUnit

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
  }
}
