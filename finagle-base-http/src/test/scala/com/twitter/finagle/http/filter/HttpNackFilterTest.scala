package com.twitter.finagle.http.filter

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.filter.ServerAdmissionControl
import com.twitter.finagle.{Failure, Service}
import com.twitter.finagle.http.{Fields, Method, Request, Response, Status}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Await, Awaitable, Duration, Future}
import java.util.concurrent.atomic.AtomicBoolean
import org.scalatest.funsuite.AnyFunSuite

class HttpNackFilterTest extends AnyFunSuite {

  private[this] def await[T](awaitable: Awaitable[T]): T =
    Await.result(awaitable, Duration.fromSeconds(30))

  test("HttpNackFilter signals that a request may be nacked if it doesn't have a body") {
    val stats = new NullStatsReceiver
    val couldBeNacked = new AtomicBoolean(false)
    val service = new HttpNackFilter(stats) andThen Service.mk { req: Request =>
      couldBeNacked.set(!Contexts.local.contains(ServerAdmissionControl.NonRetryable))
      Future.value(Response(req))
    }

    val request = Request(method = Method.Get, uri = "/foo")

    await(service(request))
    assert(couldBeNacked.get)
  }

  test("HttpNackFilter signals that a request shouldn't be nacked if it has a body") {
    val stats = new NullStatsReceiver
    val couldBeNacked = new AtomicBoolean(false)
    val service = new HttpNackFilter(stats) andThen Service.mk { req: Request =>
      couldBeNacked.set(!Contexts.local.contains(ServerAdmissionControl.NonRetryable))
      Future.value(Response(req))
    }

    val staticRequest = Request(method = Method.Post, uri = "/foo")
    staticRequest.contentString = "body"

    await(service(staticRequest))
    assert(!couldBeNacked.get)

    val streamedRequest = Request(method = Method.Post, uri = "/foo")
    streamedRequest.setChunked(true)

    await(service(streamedRequest))
    assert(!couldBeNacked.get)
  }

  test(
    "HttpNackFilter signals that a request may be nacked if it has a non-chunked body and the magic header"
  ) {
    val stats = new NullStatsReceiver
    val service = new HttpNackFilter(stats) andThen Service.mk { req: Request =>
      // header should have been stripped
      assert(!req.headerMap.contains(HttpNackFilter.RetryableRequestHeader))
      val body =
        if (Contexts.local.contains(ServerAdmissionControl.NonRetryable)) "nonretryable"
        else "retryable"

      val resp = Response(req)
      resp.contentString = body
      Future.value(resp)
    }

    val staticRequest = Request(method = Method.Post, uri = "/foo")
    staticRequest.headerMap.add(HttpNackFilter.RetryableRequestHeader, "")
    staticRequest.contentString = "body"

    val resp1 = await(service(staticRequest))
    assert(resp1.contentString == "retryable")

    val streamedRequest = Request(method = Method.Post, uri = "/foo")
    streamedRequest.headerMap.add(HttpNackFilter.RetryableRequestHeader, "")
    streamedRequest.setChunked(true)

    val resp2 = await(service(streamedRequest))
    assert(resp2.contentString == "nonretryable")
  }

  test("HttpNackFilter returns a body when rejecting a request that isn't a HEAD request") {
    val stats = new NullStatsReceiver
    val service = new HttpNackFilter(stats) andThen Service.mk { _: Request =>
      Future.exception(Failure.rejected("sadface"))
    }

    val canHaveBodies = Set(
      Method.Get,
      Method.Post,
      Method.Put,
      Method.Patch,
      Method.Delete,
      Method.Trace,
      Method.Connect,
      Method.Options
    )

    canHaveBodies.foreach { method =>
      val request = Request()
      request.method = method

      val rep = await(service(request))
      assert(rep.status == Status.ServiceUnavailable)
      assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == Some("true"))
      assert(rep.headerMap.get(Fields.RetryAfter) == Some("0"))
      assert(rep.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)
      assert(!rep.content.isEmpty)
    }
  }

  test("HttpNackFilter does not return a body when rejecting a HEAD request") {
    val stats = new NullStatsReceiver
    val service = new HttpNackFilter(stats) andThen Service.mk { _: Request =>
      Future.exception(Failure.rejected("sadface"))
    }

    val request = Request()
    request.method = Method.Head

    val rep = await(service(request))
    assert(rep.status == Status.ServiceUnavailable)
    assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == Some("true"))
    assert(rep.headerMap.get(Fields.RetryAfter) == Some("0"))
    assert(rep.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)
    assert(rep.content.isEmpty)
  }
}
