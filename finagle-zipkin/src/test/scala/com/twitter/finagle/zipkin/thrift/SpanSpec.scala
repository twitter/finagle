package com.twitter.finagle.zipkin.thrift

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.conversions.time._
import com.twitter.util.Time
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId}

class SpanSpec extends SpecificationWithJUnit with Mockito {

  "Span" should {
    "serialize properly" in {
      val ann = ZipkinAnnotation(Time.now, "value", Endpoint(1,2), Some(1.second))
      val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)
      val span = Span(traceId, Some("service"), Some("name"), Seq(ann), Seq(), Endpoint(123, 123))

      val tspan = span.toThrift
      tspan.isSetAnnotations mustEqual true
      val host = tspan.getAnnotations().get(0).getHost()
      host.getService_name() mustEqual("service")
      tspan.getAnnotations().get(0).getDuration() mustEqual 1*1000*1000
      tspan.isSetName mustEqual true
      tspan.getName() mustEqual "name"
      tspan.isSetBinary_annotations mustEqual false
      tspan.isSetId mustEqual true
      tspan.getId() mustEqual 123
      tspan.isSetParent_id mustEqual true
      tspan.getParent_id() mustEqual 123
      tspan.isSetTrace_id mustEqual true
      tspan.getTrace_id() mustEqual 123
      tspan.isDebug mustEqual true
    }
  }
}
