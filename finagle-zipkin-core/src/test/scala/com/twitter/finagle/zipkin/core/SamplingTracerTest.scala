package com.twitter.finagle.zipkin.core

import com.twitter.finagle.{Address, Name, Service}
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.param
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.tracing._
import com.twitter.util._
import java.net.InetSocketAddress
import org.mockito.Mockito._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class SamplingTracerTest extends AnyFunSuite with MockitoSugar with ScalaCheckDrivenPropertyChecks {
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

  test("SampingTracer.sampleTrace should annotate root spans with sampling Rate") {
    val underlying = new BufferingTracer()
    // max sample rate to ensure the sample is selected
    val sampleRate = 1.0f

    val tracer = new SamplingTracer(underlying, sampleRate)
    val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

    assert(tracer.sampleTrace(id) == Some(true))
    val expected = Seq(("zipkin.sampling_rate", sampleRate.toDouble))
    val actual = underlying.iterator.toList collect {
      case Record(_, _, com.twitter.finagle.tracing.Annotation.BinaryAnnotation(k, v), _) => k -> v
    }
    assert(actual == expected)
  }

  test("SamplingTracer should not annotate when sampling decision is already set") {
    val underlying = new BufferingTracer()
    // max sample rate to ensure the sample is selected
    val sampleRate = 1.0f

    val tracer = new SamplingTracer(underlying, sampleRate)
    val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))

    val expected = Seq()

    val actualBeforeSampleTrace = underlying.iterator.toList collect {
      case Record(_, _, com.twitter.finagle.tracing.Annotation.BinaryAnnotation(k, v), _) => k -> v
    }
    assert(actualBeforeSampleTrace == expected)

    assert(tracer.sampleTrace(id) == Some(true))
    val actualAfter = underlying.iterator.toList collect {
      case Record(_, _, com.twitter.finagle.tracing.Annotation.BinaryAnnotation(k, v), _) => k -> v
    }
    assert(actualAfter == expected)
  }

  test("SampingTracer.sampleTrace should not annotate spans when samplingRate is 0.0f") {
    val underlying = new BufferingTracer()
    // max sample rate to ensure the sample is selected
    val sampleRate = 0.0f
    val tracer = new SamplingTracer(underlying, sampleRate)
    val id = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

    assert(tracer.sampleTrace(id) == Some(false))
    val expected = Seq()
    val actual = underlying.iterator.toList collect {
      case Record(_, _, com.twitter.finagle.tracing.Annotation.BinaryAnnotation(k, v), _) => k -> v
    }
    assert(actual == expected)
  }

  test("SamplingTracer should not annotate samplingRate if already sampled (with Stack)") {
    def getAnnotation(tracer: BufferingTracer, name: String): Option[Record] = {
      tracer.toSeq.find { record =>
        record.annotation match {
          case a: com.twitter.finagle.tracing.Annotation.BinaryAnnotation if a.key == name => true
          case _ => false
        }
      }
    }

    object Svc extends Service[String, String] {
      def apply(str: String): Future[String] = Future.value(str)
    }

    val underlying = new BufferingTracer
    val sampleRate = 1.0f
    val tracer = new SamplingTracer(underlying, sampleRate)
    val idSampled = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), Some(true))
    val idUnsampled = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None)

    val svc = StringServer.server
      .configured(param.Tracer(tracer))
      .configured(param.Label("theServer"))
      .serve("localhost:*", Svc)

    val client = StringClient.client
      .configured(param.Tracer(tracer))
      .newService(
        Name.bound(Address(svc.boundAddress.asInstanceOf[InetSocketAddress])),
        "theClient"
      )

    Trace.letId(idSampled) {
      Await.result(client("request"))
      assert(!getAnnotation(underlying, "zipkin.sampling_rate").isDefined)
    }

    Trace.letId(idUnsampled) {
      Await.result(client("request"))
      assert(getAnnotation(underlying, "zipkin.sampling_rate").isDefined)
    }
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
