package com.twitter.finagle.b3.thrift

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.finagle.tracing.{SpanId, TraceId}

class SpanSpec extends Specification with Mockito {

  "Span" should {
    "serialize properly" in {
      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)
      val span = Span(traceId, Some("service"), Some("name"), Seq(), Map(), Some(Endpoint(123, 123)))

      val tspan = span.toThrift
      tspan.isSetAnnotations mustEqual false
      tspan.isSetService_name mustEqual true
      tspan.isSetName mustEqual true
      tspan.isSetBinary_annotations mustEqual false
      tspan.isSetId mustEqual true
      tspan.isSetParent_id mustEqual true
      tspan.isSetTrace_id mustEqual true
    }
  }
}
