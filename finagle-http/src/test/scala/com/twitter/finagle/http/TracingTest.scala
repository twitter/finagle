package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.tracing.{Flags, SpanId, Trace, TraceId}
import com.twitter.util.{Await, Future}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class TracingTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  import HttpTracing.{Header, stripParameters}

  lazy val flags = Flags().setDebug
  lazy val traceId = TraceId(Some(SpanId(1)), None, SpanId(2), Some(true), flags)
  lazy val traceId128Bit =
    TraceId(Some(SpanId(2L)), None, SpanId(2), Some(true), flags, Some(SpanId(1L)))

  test("set header") {
    Trace.letId(traceId) {
      val svc = new Service[Request, Response] {
        def apply(request: Request): Future[Response] = {
          assert(request.headerMap(Header.TraceId) == traceId.traceId.toString)
          assert(request.headerMap(Header.SpanId) == traceId.spanId.toString)
          assert(!request.headerMap.contains(Header.ParentSpanId))
          assert(request.headerMap(Header.Sampled).toBoolean == traceId.sampled.get)
          assert(request.headerMap(Header.Flags).toLong == traceId.flags.toLong)

          Future.value(Response())
        }
      }

      val req = Request("/test.json")
      TraceInfo.setClientRequestHeaders(req)
      val res = svc(req)
      assert(Status.Ok == Await.result(res, 5.seconds).status)
    }
  }

  test("record only path of url") {
    val stripped = stripParameters("/1/lists/statuses.json?count=50&super_secret=ohyeah")
    assert(stripped == "/1/lists/statuses.json")

    val invalid = stripParameters("\\")
    assert(invalid == "\\") // request path doesn't throw exceptions if url is invalid
  }

  test("parse header") {
    val svc = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        assert(Trace.id == traceId)
        assert(Trace.id.flags == flags)
        Future.value(Response())
      }
    }

    val req = Request("/test.json")
    req.headerMap.add(Header.TraceId, "0000000000000001")
    req.headerMap.add(Header.SpanId, "0000000000000002")
    req.headerMap.add(Header.Sampled, "true")
    req.headerMap.add(Header.Flags, "1")
    val res = TraceInfo.letTraceIdFromRequestHeaders(req) {
      svc(req)
    }
    assert(Status.Ok == Await.result(res, 5.seconds).status)
  }

  test("parse header (128-bit TraceIDs)") {
    val svc = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        assert(Trace.id == traceId128Bit)
        assert(Trace.id.flags == flags)
        Future.value(Response())
      }
    }

    val req = Request("/test.json")
    req.headerMap.add(Header.TraceId, "00000000000000010000000000000002")
    req.headerMap.add(Header.SpanId, "0000000000000002")
    req.headerMap.add(Header.Sampled, "true")
    req.headerMap.add(Header.Flags, "1")
    val res = TraceInfo.letTraceIdFromRequestHeaders(req) {
      svc(req)
    }
    assert(Status.Ok == Await.result(res, 5.seconds).status)
  }

  test("parse header with sampled as 1") {
    val svc = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        assert(Trace.id == traceId)
        assert(Trace.id.flags == flags)
        Future.value(Response())
      }
    }

    val req = Request("/test.json")
    req.headerMap.add(Header.TraceId, "0000000000000001")
    req.headerMap.add(Header.SpanId, "0000000000000002")
    req.headerMap.add(Header.Sampled, "1")
    req.headerMap.add(Header.Flags, "1")
    val res = TraceInfo.letTraceIdFromRequestHeaders(req) {
      svc(req)
    }
    assert(Status.Ok == Await.result(res, 5.seconds).status)
  }

  test("not parse header if no trace id") {
    val svc = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        assert(Trace.id != traceId)
        Future.value(Response())
      }
    }

    val req = Request("/test.json")
    // push span id, but no trace id
    req.headerMap.add(Header.SpanId, "0000000000000002")
    val res = TraceInfo.letTraceIdFromRequestHeaders(req) {
      svc(req)
    }
    assert(Status.Ok == Await.result(res, 5.seconds).status)
  }

  test("survive bad flags entry") {
    val svc = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        assert(Trace.id.flags == Flags())
        Future.value(Response())
      }
    }

    val req = Request("/test.json")
    req.headerMap.add(Header.TraceId, "0000000000000001")
    req.headerMap.add(Header.SpanId, "0000000000000002")
    req.headerMap.add(Header.Flags, "these aren't the droids you're looking for")
    val res = TraceInfo.letTraceIdFromRequestHeaders(req) {
      svc(req)
    }
    assert(Status.Ok == Await.result(res, 5.seconds).status)
  }

  test("survive no flags entry") {
    val svc = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        assert(Trace.id.flags == Flags())
        Future.value(Response())
      }
    }

    val req = Request("/test.json")
    req.headerMap.add(Header.TraceId, "0000000000000001")
    req.headerMap.add(Header.SpanId, "0000000000000002")
    val res = TraceInfo.letTraceIdFromRequestHeaders(req) {
      svc(req)
    }
    assert(Status.Ok == Await.result(res, 5.seconds).status)
  }

  test("hasAllRequiredHeaders with all") {
    forAll(Gen.someOf(Header.Required :+ "lol")) { headers =>
      val hm = HeaderMap()
      headers.foreach { h => hm.add(h, "1") }
      val forall = Header.Required.forall { headers.contains(_) }
      val hasReqd = Header.hasAllRequired(hm)
      assert(forall == hasReqd)
    }
  }

}
