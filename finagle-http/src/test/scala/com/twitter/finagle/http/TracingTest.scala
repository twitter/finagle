package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId, Trace}
import com.twitter.util.Future
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TracingTest extends FunSuite {
  import HttpTracing.{Header, stripParameters}

  lazy val flags = Flags().setDebug
  lazy val traceId = TraceId(Some(SpanId(1)), None, SpanId(2), Some(true), flags)

  test("set header") {
    Trace.letId(traceId) {

      val dummyService = new Service[Request, Response] {
        def apply(request: Request) = {
          assert(request.headers.get(Header.TraceId) == traceId.traceId.toString)
          assert(request.headers.get(Header.SpanId) == traceId.spanId.toString)
          assert(request.headers.contains(Header.ParentSpanId) == false)
          assert(request.headers.get(Header.Sampled).toBoolean == traceId.sampled.get)
          assert(request.headers.get(Header.Flags).toLong == traceId.flags.toLong)

          Future.value(Response())
        }
      }

      val filter = new HttpClientTracingFilter[Request, Response]("testservice")
      val req = Request("/test.json")
      filter(req, dummyService)
    }
  }

  test("record only path of url") {
    val stripped = stripParameters("/1/lists/statuses.json?count=50&super_secret=ohyeah")
    assert(stripped == "/1/lists/statuses.json")

    val invalid = stripParameters("\\")
    assert(invalid == "\\") // request path doesn't throw exceptions if url is invalid
  }

  test("parse header") {
    val dummyService = new Service[Request, Response] {
      def apply(request: Request) = {
        assert(Trace.id == traceId)
        assert(Trace.id.flags == flags)
        Future.value(Response())
      }
    }

    val filter = new HttpServerTracingFilter[Request, Response]("testservice")
    val req = Request("/test.json")
    req.headers.add(Header.TraceId, "0000000000000001")
    req.headers.add(Header.SpanId, "0000000000000002")
    req.headers.add(Header.Sampled, "true")
    req.headers.add(Header.Flags, "1")
    filter(req, dummyService)
  }

  test("not parse header if no trace id") {
    val dummyService = new Service[Request, Response] {
      def apply(request: Request) = {
        assert(Trace.id != traceId)
        Future.value(Response())
      }
    }

    val filter = new HttpServerTracingFilter[Request, Response]("testservice")
    val req = Request("/test.json")
    // push span id, but no trace id
    req.headers.add(Header.SpanId, "0000000000000002")
    filter(req, dummyService)
  }

  test("survive bad flags entry") {
    val dummyService = new Service[Request, Response] {
      def apply(request: Request) = {
        assert(Trace.id.flags == Flags())
        Future.value(Response())
      }
    }

    val filter = new HttpServerTracingFilter[Request, Response]("testservice")
    val req = Request("/test.json")
    req.headers.add(Header.TraceId, "0000000000000001")
    req.headers.add(Header.SpanId, "0000000000000002")
    req.headers.add(Header.Flags, "these aren't the droids you're looking for")
    filter(req, dummyService)
  }

  test("survive no flags entry") {
    val dummyService = new Service[Request, Response] {
      def apply(request: Request) = {
        assert(Trace.id.flags == Flags())
        Future.value(Response())
      }
    }

    val filter = new HttpServerTracingFilter[Request, Response]("testservice")
    val req = Request("/test.json")
    req.headers.add(Header.TraceId, "0000000000000001")
    req.headers.add(Header.SpanId, "0000000000000002")
    filter(req, dummyService)
  }
}
