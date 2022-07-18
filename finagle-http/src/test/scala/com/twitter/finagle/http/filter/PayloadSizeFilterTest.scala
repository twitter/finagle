package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.http.Method
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Status
import com.twitter.finagle.http.Version
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.TraceId
import com.twitter.io.Buf
import com.twitter.io.BufReader
import com.twitter.io.Reader
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

class PayloadSizeFilterTest extends AnyFunSuite with Eventually {

  private def filter(sr: StatsReceiver) =
    new PayloadSizeFilter(sr, PayloadSizeFilter.serverTraceKeyPrefix)

  private def nonStreamingService(sr: StatsReceiver) = filter(sr).andThen {
    Service.mk[Request, Response] { req =>
      Future.value {
        val rep = Response.apply(req)
        rep.contentString = "key=value2"
        rep
      }
    }
  }

  private def streamingService(sr: StatsReceiver) = filter(sr).andThen {
    Service.mk[Request, Response] { req =>
      BufReader.readAll(req.reader)
      val reader = Reader.fromSeq(List("1234", "12345", "123456", "1234567"))
      Future.value(Response.apply(Version.Http11, Status.Ok, reader.map(Buf.Utf8(_))))
    }
  }

  private val nonStreamingRequest = {
    val req = Request.apply("/test")
    req.contentString = "key=value"
    req
  }

  private def streamingRequest: Request = {
    val reader = Reader.fromSeq(List("1", "12", "123"))
    Request.apply(Version.Http11, Method.Get, "/test", reader.map(Buf.Utf8(_)))
  }

  def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  test("nonStreaming -- traces sizes when actively tracing") {
    val svc = nonStreamingService(NullStatsReceiver)
    Time.withCurrentTimeFrozen { _ =>
      val tracer = new BufferingTracer
      Trace.letTracer(tracer) {
        assert(Trace.isActivelyTracing)
        assert(await(svc(nonStreamingRequest)).contentString == "key=value2")
      }
      assert(
        tracer.toSeq == Seq(
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("srv/request_payload_bytes", 9),
            None
          ),
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("srv/response_payload_bytes", 10),
            None
          )
        )
      )
    }
  }

  test("nonStreaming -- doesn't trace sizes when not actively tracing") {
    val svc = nonStreamingService(NullStatsReceiver)
    val tracer = new BufferingTracer {
      override def isActivelyTracing(traceId: TraceId): Boolean = false
    }
    Trace.letTracer(tracer) {
      assert(!Trace.isActivelyTracing)
      assert(await(svc(nonStreamingRequest)).contentString == "key=value2")
    }
    assert(tracer.toSeq == Nil)
  }

  test("nonStreaming -- records metrics") {
    val stats = new InMemoryStatsReceiver()
    val svc = nonStreamingService(stats)
    assert(await(svc(nonStreamingRequest)).contentString == "key=value2")
    assert(stats.stat("request_payload_bytes")() == Seq(9f))
    eventually {
      assert(stats.stat("response_payload_bytes")() == Seq(10f))
    }
  }

  test("streaming -- traces sizes when actively tracing") {
    val svc = streamingService(NullStatsReceiver)
    Time.withCurrentTimeFrozen { _ =>
      val tracer = new BufferingTracer
      Trace.letTracer(tracer) {
        assert(Trace.isActivelyTracing)
        val rep = await(svc(streamingRequest))
        assert(
          Buf.Utf8.unapply(await(BufReader.readAll(rep.reader))) == Some(
            "1234" + "12345" + "123456" + "1234567"))
      }
      assert(
        tracer.toSeq == Seq(
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("srv/stream/request/chunk_payload_bytes", 6),
            None
          ),
          Record(
            Trace.id,
            Time.Bottom,
            Annotation.BinaryAnnotation("srv/stream/response/chunk_payload_bytes", 22),
            None
          )
        )
      )
    }
  }

  test("streaming -- doesn't trace sizes when not actively tracing") {
    val svc = streamingService(NullStatsReceiver)
    val tracer = new BufferingTracer {
      override def isActivelyTracing(traceId: TraceId): Boolean = false
    }
    Trace.letTracer(tracer) {
      assert(!Trace.isActivelyTracing)
      val rep = await(svc(streamingRequest))
      assert(
        Buf.Utf8.unapply(await(BufReader.readAll(rep.reader))) == Some(
          "1234" + "12345" + "123456" + "1234567"))
    }
    assert(tracer.toSeq == Nil)
  }

  test("streaming -- records metrics") {
    val stats = new InMemoryStatsReceiver()
    val svc = streamingService(stats)
    val rep = await(svc(streamingRequest))
    assert(
      Buf.Utf8.unapply(await(BufReader.readAll(rep.reader))) == Some(
        "1234" + "12345" + "123456" + "1234567"))
    assert(stats.stat("stream", "request", "chunk_payload_bytes")() == Seq(1f, 2f, 3f))
    eventually {
      BufReader.readAll(rep.reader)
      assert(stats.stat("stream", "response", "chunk_payload_bytes")() == Seq(4f, 5f, 6f, 7f))
    }
  }
}
