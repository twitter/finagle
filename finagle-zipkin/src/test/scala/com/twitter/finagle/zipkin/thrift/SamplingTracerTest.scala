package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.tracing._
import com.twitter.util.Time
import com.twitter.util.events.Sink
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SamplingTracerTest extends FunSuite
  with MockitoSugar
{

  private val traceId = TraceId(
    None,
    None,
    new SpanId(1L),
    None,
    Flags()
  )

  private val record = Record(
    traceId,
    Time.now,
    Annotation.Message("sup"),
    None
  )

  test("sends sampled events to Sink") {
    val sink = mock[Sink]
    when(sink.recording).thenReturn(true)
    val tracer = mock[Tracer]
    val samplingTracer = new SamplingTracer(tracer, 1f, sink)
    samplingTracer.record(record)

    verify(sink, times(1)).event(ZipkinTracer.Trace, objectVal = record.annotation)
  }

  test("does not send events to sink when not sampled") {
    val sink = mock[Sink]
    val tracer = mock[Tracer]
    val samplingTracer = new SamplingTracer(tracer, 0f, sink)
    samplingTracer.record(record)

    verifyNoMoreInteractions(sink)
  }

}
