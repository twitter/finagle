package com.twitter.finagle.zipkin.thrift

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Service, ChannelWriteException}
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.scribe.thriftscala.{LogEntry, ResultCode, Scribe}
import com.twitter.finagle.tracing._
import com.twitter.finagle.zipkin.core.{BinaryAnnotation, Endpoint, Span, ZipkinAnnotation}
import com.twitter.util.{Await, Awaitable, Future, MockTimer, Time}
import java.net.{InetAddress, InetSocketAddress}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class ScribeRawZipkinTracerTest extends FunSuite with MockitoSugar {

  def await[A](a: Awaitable[A]): A = Await.result(a, 5.seconds)

  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)

  class ScribeClient extends Scribe.MethodPerEndpoint {
    var messages: Seq[LogEntry] = Seq.empty[LogEntry]
    var response: Future[ResultCode] = Future.value(ResultCode.Ok)
    def log(msgs: scala.collection.Seq[LogEntry]): Future[ResultCode] = {
      messages ++= msgs
      response
    }
  }

  test("exhaustively retries TryLater responses") {
    val sr = new InMemoryStatsReceiver()
    val scribeStats = new ScribeStats(sr)
    val alwaysTryLaterSvc = Service.const(Future.value(ResultCode.TryLater))
    val clnt =
      ScribeRawZipkinTracer.newClient(
        "",
        1234,
        scribeStats,
        "zipkin-scribe",
        sr.scope("clnt"),
        Some(alwaysTryLaterSvc))
    await(clnt.log(Seq.empty))

    assert(sr.counter("scribe", "try_later")() == 3)
    assert(sr.counter("scribe", "ok")() == 0)
    assert(sr.stat("clnt", "zipkin-scribe", "retries")().map(_.toInt) == Seq(2))
  }

  test("marks unknown result code as non-retryable failures") {
    val sr = new InMemoryStatsReceiver()
    val scribeStats = new ScribeStats(sr)
    val unknownResultCodeSvc = Service.const(Future.value(ResultCode.EnumUnknownResultCode(100)))
    val clnt =
      ScribeRawZipkinTracer.newClient(
        "",
        1234,
        scribeStats,
        "zipkin-scribe",
        sr.scope("clnt"),
        Some(unknownResultCodeSvc))
    await(clnt.log(Seq.empty))

    assert(sr.counter("clnt", "zipkin-scribe", "logical", "requests")() == 1)
    assert(sr.counter("clnt", "zipkin-scribe", "logical", "failures")() == 1)
    assert(sr.counter("scribe", "error", "EnumUnknownResultCode100")() == 1)
    assert(sr.counter("scribe", "try_later")() == 0)
    assert(sr.counter("scribe", "ok")() == 0)
    assert(sr.stat("clnt", "zipkin-scribe", "retries")().map(_.toInt) == Seq(0))
  }

  test("does not retry failures handled by the RequeueFilter") {
    val sr = new InMemoryStatsReceiver()
    val scribeStats = new ScribeStats(sr)
    val failingLogSvc = Service.const(Future.exception(new ChannelWriteException(None)))
    val clnt =
      ScribeRawZipkinTracer.newClient(
        "",
        1234,
        scribeStats,
        "zipkin-scribe",
        sr.scope("clnt"),
        Some(failingLogSvc))
    intercept[ChannelWriteException] {
      await(clnt.log(Seq.empty))
    }

    assert(sr.counter("clnt", "zipkin-scribe", "logical", "requests")() == 1)
    assert(sr.counter("clnt", "zipkin-scribe", "logical", "failures")() == 1)
    assert(sr.stat("clnt", "zipkin-scribe", "retries")().map(_.toInt) == Seq(0))
  }

  test("reports correct metrics after a successful retry") {
    val sr = new InMemoryStatsReceiver()
    val scribeStats = new ScribeStats(sr)
    val tryLaterThenOkSvc = mock[Service[Scribe.Log.Args, ResultCode]]
    when(tryLaterThenOkSvc.apply(any[Scribe.Log.Args]))
      .thenReturn(Future.value(ResultCode.TryLater))
      .thenReturn(Future.value(ResultCode.Ok))

    val clnt =
      ScribeRawZipkinTracer.newClient(
        "",
        1234,
        scribeStats,
        "zipkin-scribe",
        sr.scope("clnt"),
        Some(tryLaterThenOkSvc))
    await(clnt.log(Seq.empty))

    assert(sr.counter("clnt", "zipkin-scribe", "logical", "requests")() == 1)
    assert(sr.counter("clnt", "zipkin-scribe", "logical", "success")() == 1)
    assert(sr.counter("scribe", "try_later")() == 1)
    assert(sr.counter("scribe", "ok")() == 1)
    assert(sr.stat("clnt", "zipkin-scribe", "retries")().map(_.toInt) == Seq(1))
  }

  test("reports errors") {
    val sr = new InMemoryStatsReceiver()
    val scribeStats = new ScribeStats(sr)
    val failingLogSvc = Service.const(Future.exception(new IllegalArgumentException))
    val clnt =
      ScribeRawZipkinTracer.newClient(
        "",
        1234,
        scribeStats,
        "zipkin-scribe",
        sr.scope("clnt"),
        Some(failingLogSvc))
    intercept[IllegalArgumentException] {
      await(clnt.log(Seq.empty))
    }

    assert(sr.counter("clnt", "zipkin-scribe", "logical", "requests")() == 1)
    assert(sr.counter("clnt", "zipkin-scribe", "logical", "failures")() == 1)
    assert(sr.counter("scribe", "try_later")() == 0)
    assert(sr.counter("scribe", "ok")() == 0)
    assert(sr.counter("scribe", "error", "java.lang.IllegalArgumentException")() == 1)
  }

  test("formulate scribe log message correctly") {
    val scribe = new ScribeClient
    val tracer = new ScribeRawZipkinTracer(scribe, ScribeStats.empty)

    val localEndpoint = Endpoint(2323, 23)
    val remoteEndpoint = Endpoint(333, 22)

    val annotations = Seq(
      ZipkinAnnotation(Time.fromSeconds(123), "cs", localEndpoint),
      ZipkinAnnotation(Time.fromSeconds(126), "cr", localEndpoint),
      ZipkinAnnotation(Time.fromSeconds(123), "ss", remoteEndpoint),
      ZipkinAnnotation(Time.fromSeconds(124), "sr", remoteEndpoint),
      ZipkinAnnotation(Time.fromSeconds(123), "llamas", localEndpoint)
    )

    val span = Span(
      traceId = traceId,
      annotations = annotations,
      _serviceName = Some("hickupquail"),
      _name = Some("foo"),
      bAnnotations = Seq.empty[BinaryAnnotation],
      endpoint = localEndpoint
    )

    val expected = LogEntry(
      category = "zipkin",
      message = "CgABAAAAAAAAAHsLAAMAAAADZm9vCgAEAAAAAAAAAHsKAAUAAAAAAAAAe" +
        "w8ABgwAAAAFCgABAAAAAAdU1MALAAIAAAACY3MMAAMIAAEAAAkTBgACABcLAAMAAAA" +
        "LaGlja3VwcXVhaWwAAAoAAQAAAAAHgpuACwACAAAAAmNyDAADCAABAAAJEwYAAgAXC" +
        "wADAAAAC2hpY2t1cHF1YWlsAAAKAAEAAAAAB1TUwAsAAgAAAAJzcwwAAwgAAQAAAU0" +
        "GAAIAFgsAAwAAAAtoaWNrdXBxdWFpbAAACgABAAAAAAdkFwALAAIAAAACc3IMAAMIA" +
        "AEAAAFNBgACABYLAAMAAAALaGlja3VwcXVhaWwAAAoAAQAAAAAHVNTACwACAAAABmx" +
        "sYW1hcwwAAwgAAQAACRMGAAIAFwsAAwAAAAtoaWNrdXBxdWFpbAAAAgAJAQoACgAAAA" +
        "AHVNTAAA==\n"
    )

    tracer.sendSpans(Seq(span))
    assert(scribe.messages == Seq(expected))
  }

  test("send all traces to scribe") {
    Time.withCurrentTimeFrozen { tc =>
      val scribe = new ScribeClient
      val timer = new MockTimer
      val tracer =
        new ScribeRawZipkinTracer(scribe, ScribeStats.empty, timer = timer)

      val localAddress = InetAddress.getByAddress(Array.fill(4) {
        1
      })
      val remoteAddress = InetAddress.getByAddress(Array.fill(4) {
        10
      })
      val port1 = 80 // never bound
      val port2 = 53 // ditto
      tracer.record(
        Record(
          traceId,
          Time.now,
          Annotation.ClientAddr(new InetSocketAddress(localAddress, port1))
        )
      )
      tracer.record(
        Record(
          traceId,
          Time.now,
          Annotation.LocalAddr(new InetSocketAddress(localAddress, port1))
        )
      )
      tracer.record(
        Record(
          traceId,
          Time.now,
          Annotation.ServerAddr(new InetSocketAddress(remoteAddress, port2))
        )
      )
      tracer.record(Record(traceId, Time.now, Annotation.ServiceName("service")))
      tracer.record(Record(traceId, Time.now, Annotation.Rpc("method")))
      tracer.record(
        Record(traceId, Time.now, Annotation.BinaryAnnotation("i16", 16.toShort))
      )
      tracer.record(Record(traceId, Time.now, Annotation.BinaryAnnotation("i32", 32)))
      tracer.record(Record(traceId, Time.now, Annotation.BinaryAnnotation("i64", 64L)))
      tracer.record(
        Record(traceId, Time.now, Annotation.BinaryAnnotation("double", 123.3d))
      )
      tracer.record(
        Record(traceId, Time.now, Annotation.BinaryAnnotation("string", "woopie"))
      )
      tracer.record(Record(traceId, Time.now, Annotation.Message("boo")))
      tracer.record(
        Record(traceId, Time.now, Annotation.Message("boohoo"), Some(1.second))
      )
      tracer.record(Record(traceId, Time.now, Annotation.ClientSend))
      tracer.record(Record(traceId, Time.now, Annotation.ClientRecv))

      tc.set(Time.Top) // advance timer enough to guarantee spans are logged
      timer.tick()

      // Note: Since ports are ephemeral, we can't hardcode expected message.
      assert(scribe.messages.size >= 1)
    }
  }

  test("logSpan if a timeout occurs") {
    Time.withCurrentTimeFrozen { tc =>
      val ann1 = Annotation.Message("some_message")
      val ann2 = Annotation.ServiceName("some_service")
      val ann3 = Annotation.Rpc("rpc_name")
      val ann4 = Annotation.Message(TimeoutFilter.TimeoutAnnotation)

      val scribe = new ScribeClient
      val timer = new MockTimer
      val tracer =
        new ScribeRawZipkinTracer(scribe, ScribeStats.empty, timer = timer)

      tracer.record(Record(traceId, Time.fromSeconds(1), ann1))
      tracer.record(Record(traceId, Time.fromSeconds(2), ann2))
      tracer.record(Record(traceId, Time.fromSeconds(3), ann3))
      tracer.record(Record(traceId, Time.fromSeconds(3), ann4))

      tc.set(Time.Top) // advance timer enough to guarantee spans are logged
      timer.tick()

      // scribe Log method is in java
      assert(scribe.messages.size >= 1)
    }
  }
}
