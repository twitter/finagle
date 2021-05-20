package com.twitter.finagle.zipkin.core

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.tracing._
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

class RawZipkinTracerTest extends AnyFunSuite {

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)

  class FakeRawZipkinTracer extends RawZipkinTracer {
    var spans: Seq[Span] = Seq.empty
    override def sendSpans(xs: Seq[Span]): Future[Unit] = {
      spans ++= xs
      Future.Unit
    }
  }

  test("send spans when flushed") {
    val tracer = new FakeRawZipkinTracer

    val localAddress = InetAddress.getByAddress(Array.fill(4) { 1 })
    val remoteAddress = InetAddress.getByAddress(Array.fill(4) { 10 })
    val port1 = 80 // never bound
    val port2 = 53 // ditto
    tracer.record(
      Record(
        traceId,
        Time.fromSeconds(123),
        Annotation.ClientAddr(new InetSocketAddress(localAddress, port1))
      )
    )
    tracer.record(
      Record(
        traceId,
        Time.fromSeconds(123),
        Annotation.LocalAddr(new InetSocketAddress(localAddress, port1))
      )
    )
    tracer.record(
      Record(
        traceId,
        Time.fromSeconds(123),
        Annotation.ServerAddr(new InetSocketAddress(remoteAddress, port2))
      )
    )
    tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ServiceName("service")))
    tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Rpc("method")))
    tracer.record(
      Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("i16", 16.toShort))
    )
    tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("i32", 32)))
    tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("i64", 64L)))
    tracer.record(
      Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("double", 123.3d))
    )
    tracer.record(
      Record(traceId, Time.fromSeconds(123), Annotation.BinaryAnnotation("string", "woopie"))
    )
    tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.Message("boo")))
    tracer.record(
      Record(traceId, Time.fromSeconds(123), Annotation.Message("boohoo"), Some(1.second))
    )
    tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientSend))
    tracer.record(Record(traceId, Time.fromSeconds(123), Annotation.ClientRecv))

    tracer.flush()

    assert(tracer.spans.length == 1)
  }
}
