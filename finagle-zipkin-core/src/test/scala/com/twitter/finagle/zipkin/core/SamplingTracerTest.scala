package com.twitter.finagle.zipkin.core

import com.twitter.finagle.tracing._
import com.twitter.util._
import org.mockito.Mockito._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import java.net.InetSocketAddress

class SamplingTracerTest extends FunSuite with MockitoSugar with ScalaCheckDrivenPropertyChecks {
  test("SamplingTracer should handle sampling") {
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

    val underlying = mock[RawZipkinTracer]
    val tracer = new SamplingTracer(underlying, 0f)
    assert(tracer.getSampleRate == 0f)
    assert(tracer.sampleTrace(traceId).contains(false))
    tracer.setSampleRate(1f)
    assert(tracer.getSampleRate == 1f)
    assert(tracer.sampleTrace(traceId).contains(true))
  }

  test("SamplingTracer should pass through trace id with sampled true despite sample rate") {
    val underlying = mock[RawZipkinTracer]
    val tracer = new SamplingTracer(underlying, 0f)
    val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))
    val record = Record(id, Time.now, Annotation.ClientSend)
    tracer.record(record)
    verify(underlying).record(record)
  }

  test("SamplingTracer should return isActivelyTracing correctly based on sampled value") {
    val underlying = mock[RawZipkinTracer]
    val tracer = new SamplingTracer(underlying, 0f)
    val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))
    val record = Record(id, Time.now, Annotation.ClientSend)

    // true when sampled is true
    assert(tracer.isActivelyTracing(id))
    // true when sampled is not set
    assert(tracer.isActivelyTracing(id.copy(_sampled = None)))
    // true when debug is set
    assert(tracer.isActivelyTracing(id.copy(_sampled = Some(false), flags = Flags(Flags.Debug))))
    // false when sampled is false
    assert(!tracer.isActivelyTracing(id.copy(_sampled = Some(false))))
  }
}

private[twitter] object SamplingTracerTest {
  import Annotation._
  import Arbitrary.arbitrary

  val genAnnotation: Gen[Annotation] = Gen.oneOf(
    Gen.oneOf(ClientSend, ClientRecv, ServerSend, ServerRecv),
    Gen.oneOf(
      ClientSendFragment,
      ClientRecvFragment,
      ServerSendFragment,
      ServerRecvFragment
    ),
    for (s <- arbitrary[String]) yield Message(s),
    for (s <- arbitrary[String]) yield ServiceName(s),
    for (s <- arbitrary[String]) yield Rpc(s),
    Gen.oneOf(
      ClientAddr(new InetSocketAddress(0)),
      ServerAddr(new InetSocketAddress(0)),
      LocalAddr(new InetSocketAddress(0))
    ),
    // We only guarantee successful deserialization for primitive values and
    // Strings, here we test String.
    for (v <- Gen.oneOf(arbitrary[AnyVal], arbitrary[String])) yield BinaryAnnotation("k", v)
  )
}
