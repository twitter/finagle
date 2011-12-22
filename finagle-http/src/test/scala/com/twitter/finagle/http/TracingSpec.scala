package com.twitter.finagle.http

import org.specs.Specification
import com.twitter.finagle.Service
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpResponse, HttpRequest}
import com.twitter.finagle.tracing.{SpanId, TraceId, Trace}
import java.net.InetSocketAddress
import HttpTracing._

object TracingSpec extends Specification {

  val traceId = TraceId(Some(SpanId(1)), None, SpanId(2), Some(true))

  "TracingFilters" should {
    "set header" in {
      Trace.pushId(traceId)

      val dummyService = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = {
          request.getHeader(Header.TraceId) mustEqual traceId.traceId.toString
          request.getHeader(Header.SpanId) mustEqual traceId.spanId.toString
          request.containsHeader(Header.ParentSpanId) mustEqual false
          request.getHeader(Header.Sampled).toBoolean mustEqual traceId.sampled.get

          Future.value(Response())
        }
      }

      val filter = new HttpClientTracingFilter[HttpResponse]("testservice")
      val req = Request("/test.json")
      filter(req, dummyService)
    }

    "parse header" in {
      val dummyService = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = {
          Trace.id mustEqual traceId
          Future.value(Response())
        }
      }

      val addr = new InetSocketAddress(0)
      val filter = new HttpServerTracingFilter("testservice", addr)
      val req = Request("/test.json")
      req.addHeader(Header.TraceId, "0000000000000001")
      req.addHeader(Header.SpanId, "0000000000000002")
      req.addHeader(Header.Sampled, "true")
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
      val filter = new HttpServerTracingFilter("testservice", addr)
      val req = Request("/test.json")
      // push span id, but no trace id
      req.addHeader(Header.SpanId, "0000000000000002")
      filter(req, dummyService)
    }
  }

}
