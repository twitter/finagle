package com.twitter.finagle.b3.thrift

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.finagle.tracing.{SpanId, TraceId}
import com.twitter.util.Time

class SpanSpec extends Specification with Mockito {

  "Span" should {
    "serialize properly" in {
      val ann = B3Annotation(Time.now, "value", Endpoint(1,2))
      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)
      val span = Span(traceId, Some("service"), Some("name"), Seq(ann), Map(), Some(Endpoint(123, 123)))

      val tspan = span.toThrift
      tspan.isSetAnnotations mustEqual true
      val host = tspan.getAnnotations().get(0).getHost()
      host.getService_name() mustEqual("service")
      tspan.isSetName mustEqual true
      tspan.getName() mustEqual "name"
      tspan.isSetBinary_annotations mustEqual false
      tspan.isSetId mustEqual true
      tspan.getId() mustEqual 123
      tspan.isSetParent_id mustEqual true
      tspan.getParent_id() mustEqual 123
      tspan.isSetTrace_id mustEqual true
      tspan.getTrace_id() mustEqual 123
    }
  }
}
