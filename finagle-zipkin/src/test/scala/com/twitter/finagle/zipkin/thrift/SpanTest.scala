package com.twitter.finagle.zipkin.thrift

import org.scalatest.FunSuite
import com.twitter.util.Time
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class SpanTest extends FunSuite {
  test("Span should serialize properly") {
    val ann = ZipkinAnnotation(Time.now, "value", Endpoint(1, 2))
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)
    val span = Span(traceId, Some("service"), Some("name"), Seq(ann), Seq(), Endpoint(123, 123))

    val tspan = span.toThrift
    assert(tspan.isSetAnnotations)
    val host = tspan.getAnnotations.get(0).getHost
    assert(host.getService_name == "service")
    assert(tspan.isSetName)
    assert(tspan.getName == "name")
    !tspan.isSetBinary_annotations
    assert(tspan.isSetId)
    assert(tspan.getId == 123)
    assert(tspan.isSetParent_id)
    assert(tspan.getParent_id == 123)
    assert(tspan.isSetTrace_id)
    assert(tspan.getTrace_id == 123)
    assert(tspan.isDebug)
  }
}
