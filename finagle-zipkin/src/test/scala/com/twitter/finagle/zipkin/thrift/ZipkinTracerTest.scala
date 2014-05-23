package com.twitter.finagle.zipkin.thrift

import org.scalatest.mock.MockitoSugar
import org.scalatest.FunSuite
import org.mockito.Mockito.verify
import com.twitter.util._
import com.twitter.finagle.tracing._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class ZipkinTracerTest extends FunSuite with MockitoSugar {
  test("ZipkinTracer should handle sampling") {
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

    val underlying = mock[RawZipkinTracer]
    val tracer = new ZipkinTracer(underlying, 0f)
    assert(tracer.sampleTrace(traceId) === Some(false))
    tracer.setSampleRate(1f)
    assert(tracer.sampleTrace(traceId) === Some(true))
  }

  test("ZipkinTracer should pass through trace id with sampled true despite of sample rate") {
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

    val underlying = mock[RawZipkinTracer]
    val tracer = new ZipkinTracer(underlying, 0f)
    val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))
    val record = Record(id, Time.now, Annotation.ClientSend())
    tracer.record(record)
    verify(underlying).record(record)
  }
}
