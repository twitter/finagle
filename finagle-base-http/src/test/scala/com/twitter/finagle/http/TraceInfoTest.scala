package com.twitter.finagle.http

import com.twitter.finagle.tracing._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TraceInfoTest extends FunSuite {

  // The only use-case for flags is the debug flag. Don't burn headers on an edge case
  test("setClientRequestHeaders doesn't set header on default flags") {
    val traceContext = TraceId(None, None, SpanId(0xabc), None)
    val req = Request(Method.Get, "/")

    Trace.letId(traceContext) {
      TraceInfo.setClientRequestHeaders(req)
    }

    assert(req.headerMap.get("X-B3-Flags").isEmpty)
  }

  // None is represented by lack of header in B3
  test("setClientRequestHeaders doesn't set header on no sampling decision") {
    val sampled = None
    val traceContext = TraceId(None, None, SpanId(0xabc), sampled)
    val req = Request(Method.Get, "/")

    Trace.letId(traceContext) {
      TraceInfo.setClientRequestHeaders(req)
    }

    assert(req.headerMap.get("X-B3-Sampled").isEmpty)
  }

  // It is important to propagate IDs downstream even when not sampled, for log correlation
  test("setClientRequestHeaders with parent ID even when not sampled") {
    val req = Request(Method.Get, "/")

    val traceContext =
      TraceId(Some(SpanId(0xabc)), Some(SpanId(0xdef)), SpanId(0x123), Some(false), Flags(0))
    Trace.letId(traceContext) {
      TraceInfo.setClientRequestHeaders(req)
    }

    assert(req.headerMap == HeaderMap(
      "X-B3-TraceId" -> "0000000000000abc",
      "X-B3-ParentSpanId" -> "0000000000000def",
      "X-B3-SpanId" -> "0000000000000123",
      "X-B3-Sampled" -> "false"
    ))
  }

  // Particularly headers like parent ID need to be removed when a request is processed twice
  test("setClientRequestHeaders clears old headers when no trace") {
    val req = Request(Method.Get, "/")
    req.headerMap.put("X-B3-TraceId", "0000000000000abc")
    req.headerMap.put("X-B3-ParentSpanId", "0000000000000def")
    req.headerMap.put("X-B3-SpanId", "0000000000000123")
    req.headerMap.put("X-B3-Sampled", "false")
    req.headerMap.put("X-B3-Flags", "1")

    // clears and starts an unsampled trace
    TraceInfo.setClientRequestHeaders(req)

    assert(req.headerMap.keys == Set( "X-B3-TraceId", "X-B3-SpanId"))
  }

  test("setClientRequestHeaders writes 128-bit trace ID") {
    val req = Request(Method.Get, "/")

    val traceContext = TraceId(Some(SpanId(0xb)), None, SpanId(0x1), None, Flags(), Some(SpanId(0xa)))
    Trace.letId(traceContext) {
      TraceInfo.setClientRequestHeaders(req)
    }

    assert(req.headerMap == HeaderMap(
      "X-B3-TraceId" -> "000000000000000a000000000000000b",
      "X-B3-SpanId" -> "0000000000000001"
    ))
  }
}
