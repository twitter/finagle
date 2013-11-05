package com.twitter.finagle.http

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.Service
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpResponse, HttpRequest}
import java.net.InetSocketAddress
import HttpTracing._
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId, Trace}

class TracingSpec extends SpecificationWithJUnit {

  val flags = Flags().setDebug
  val traceId = TraceId(Some(SpanId(1)), None, SpanId(2), Some(true), flags)

  "TracingFilters" should {
    "set header" in {
      Trace.unwind {
        Trace.setId(traceId)

        val dummyService = new Service[HttpRequest, HttpResponse] {
          def apply(request: HttpRequest) = {
            request.getHeader(Header.TraceId) mustEqual traceId.traceId.toString
            request.getHeader(Header.SpanId) mustEqual traceId.spanId.toString
            request.containsHeader(Header.ParentSpanId) mustEqual false
            request.getHeader(Header.Sampled).toBoolean mustEqual traceId.sampled.get
            request.getHeader(Header.Flags).toLong mustEqual traceId.flags.toLong

            Future.value(Response())
          }
        }

        val filter = new HttpClientTracingFilter[HttpRequest, HttpResponse]("testservice")
        val req = Request("/test.json")
        filter(req, dummyService)
      }
    }

    "record only path of url" in {
      val stripped = stripParameters("/1/lists/statuses.json?count=50&super_secret=ohyeah")
      stripped mustEqual "/1/lists/statuses.json"

      val invalid = stripParameters("\\")
      invalid mustEqual "\\" // request path doesn't throw exceptions if url is invalid
    }

    "parse header" in {
      val dummyService = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = {
          Trace.id mustEqual traceId
          Trace.id.flags mustEqual flags
          Future.value(Response())
        }
      }

      val addr = new InetSocketAddress(0)
      val filter = new HttpServerTracingFilter[HttpRequest, HttpResponse]("testservice", addr)
      val req = Request("/test.json")
      req.addHeader(Header.TraceId, "0000000000000001")
      req.addHeader(Header.SpanId, "0000000000000002")
      req.addHeader(Header.Sampled, "true")
      req.addHeader(Header.Flags, "1")
      filter(req, dummyService)
    }

    "not parse header if no trace id" in {
      val dummyService = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = {
          Trace.id mustNotEq traceId
          Future.value(Response())
        }
      }

      val addr = new InetSocketAddress(0)
      val filter = new HttpServerTracingFilter[HttpRequest, HttpResponse]("testservice", addr)
      val req = Request("/test.json")
      // push span id, but no trace id
      req.addHeader(Header.SpanId, "0000000000000002")
      filter(req, dummyService)
    }

    "survive bad flags entry" in {
      val dummyService = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = {
          Trace.id.flags mustEqual Flags()
          Future.value(Response())
        }
      }

      val addr = new InetSocketAddress(0)
      val filter = new HttpServerTracingFilter[HttpRequest, HttpResponse]("testservice", addr)
      val req = Request("/test.json")
      req.addHeader(Header.TraceId, "0000000000000001")
      req.addHeader(Header.SpanId, "0000000000000002")
      req.addHeader(Header.Flags, "these aren't the droids you're looking for")
      filter(req, dummyService)
    }

    "survive no flags entry" in {
      val dummyService = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = {
          Trace.id.flags mustEqual Flags()
          Future.value(Response())
        }
      }

      val addr = new InetSocketAddress(0)
      val filter = new HttpServerTracingFilter[HttpRequest, HttpResponse]("testservice", addr)
      val req = Request("/test.json")
      req.addHeader(Header.TraceId, "0000000000000001")
      req.addHeader(Header.SpanId, "0000000000000002")
      filter(req, dummyService)
    }
  }

}
