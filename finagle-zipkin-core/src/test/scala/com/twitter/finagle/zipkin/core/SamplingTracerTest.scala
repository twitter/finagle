package com.twitter.finagle.zipkin.core

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

    verify(sink, times(1)).event(SamplingTracer.Trace, objectVal = record.annotation)
  }

  test("does not send events to sink when not sampled") {
    val sink = mock[Sink]
    val tracer = mock[Tracer]
    val samplingTracer = new SamplingTracer(tracer, 0f, sink)
    samplingTracer.record(record)

    verifyNoMoreInteractions(sink)
  }
}

import com.twitter.finagle.tracing._
import com.twitter.util._
import com.twitter.util.events
import org.junit.runner.RunWith
import org.mockito.Mockito.verify
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class ZipkinTracerTest extends FunSuite with MockitoSugar with GeneratorDrivenPropertyChecks {
  import ZipkinTracerTest._

  test("ZipkinTracer should handle sampling") {
    val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

    val underlying = mock[RawZipkinTracer]
    val tracer = new SamplingTracer(underlying, 0f)
    assert(tracer.sampleTrace(traceId) == Some(false))
    tracer.setSampleRate(1f)
    assert(tracer.sampleTrace(traceId) == Some(true))
  }

  test("ZipkinTracer should pass through trace id with sampled true despite of sample rate") {
    val underlying = mock[RawZipkinTracer]
    val tracer = new SamplingTracer(underlying, 0f)
    val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))
    val record = Record(id, Time.now, Annotation.ClientSend())
    tracer.record(record)
    verify(underlying).record(record)
  }

  test("ZipkinTracer should return isActivelyTracing correctly based on sampled value") {
    val underlying = mock[RawZipkinTracer]
    val tracer = new SamplingTracer(underlying, 0f)
    val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))
    val record = Record(id, Time.now, Annotation.ClientSend())

    // true when sampled is true
    assert(tracer.isActivelyTracing(id))
    // true when sampled is not set
    assert(tracer.isActivelyTracing(id.copy(_sampled = None)))
    // true when debug is set
    assert(tracer.isActivelyTracing(id.copy(_sampled = Some(false), flags = Flags(Flags.Debug))))
    // false when sampled is false
    assert(!tracer.isActivelyTracing(id.copy(_sampled = Some(false))))
  }

  test("serialize andThen deserialize = identity") {
    import SamplingTracer.Trace

    def id(e: events.Event) = Trace.serialize(e).flatMap(Trace.deserialize).get
    forAll(genEvent(Trace)) { event =>
      event.objectVal match {
        case _: Annotation.BinaryAnnotation =>
          intercept[IllegalArgumentException] { id(event) }
        case _ => assert(id(event) == event)
      }
    }
  }
}

private[twitter] object ZipkinTracerTest {
  import Annotation._
  import Arbitrary.arbitrary

  val genAnnotation: Gen[Annotation] = Gen.oneOf(
    Gen.oneOf(ClientSend(), ClientRecv(), ServerSend(), ServerRecv()),
    Gen.oneOf(
      ClientSendFragment(),
      ClientRecvFragment(),
      ServerSendFragment(),
      ServerRecvFragment()),
    for (s <- arbitrary[String]) yield Message(s),
    for (s <- arbitrary[String]) yield ServiceName(s),
    for (s <- arbitrary[String]) yield Rpc(s),
    Gen.oneOf(
      ClientAddr(new InetSocketAddress(0)),
      ServerAddr(new InetSocketAddress(0)),
      LocalAddr(new InetSocketAddress(0))),
    // We only guarantee successful deserialization for primitive values and
    // Strings, here we test String.
    for (v <- Gen.oneOf(arbitrary[AnyVal], arbitrary[String])) yield BinaryAnnotation("k", v)
  )

  def genEvent(etype: events.Event.Type): Gen[events.Event] = for {
    ann <- genAnnotation
    tid <- arbitrary[Long]
    sid <- arbitrary[Long]
  } yield events.Event(etype, Time.now, objectVal = ann,
    traceIdVal = tid, spanIdVal = sid)
}
