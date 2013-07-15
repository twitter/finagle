package com.twitter.finagle.zipkin.thrift

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import com.twitter.util._
import com.twitter.finagle.tracing._


class ZipkinTracerSpec extends SpecificationWithJUnit with Mockito {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

  "ZipkinTracer" should {

    "handle sampling" in {
      val underlying = mock[RawZipkinTracer]
      val tracer = new ZipkinTracer(underlying, 0f)
      tracer.sampleTrace(traceId) mustEqual Some(false)
      tracer.setSampleRate(1f)
      tracer.sampleTrace(traceId) mustEqual Some(true)
    }

    "pass through trace id with sampled true despite of sample rate" in {
      val underlying = mock[RawZipkinTracer]
      val tracer = new ZipkinTracer(underlying, 0f)
      val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))
      val record = Record(id, Time.now, Annotation.ClientSend())
      tracer.record(record)
      one(underlying).record(record)
    }

  }
}
