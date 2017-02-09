package com.twitter.finagle.http.filter

import com.twitter.finagle.{Failure, Service}
import com.twitter.finagle.http.{Method, Request, Status}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Await, Future, Duration}
import org.scalatest.FunSuite

class HttpNackFilterTest extends FunSuite {
  val timeout = Duration.fromSeconds(30)

  test("HttpNackFilter returns a body when rejecting a request that isn't a HEAD request") {
    val stats = new NullStatsReceiver
    val service = new HttpNackFilter(stats) andThen Service.mk { _: Request => Future.exception(Failure.rejected("sadface")) }

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

      val rep = Await.result(service(request), timeout)
      assert(rep.status == Status.ServiceUnavailable)
      assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == Some("true"))
      assert(rep.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)
      assert(!rep.content.isEmpty)
    }
  }

  test("HttpNackFilter does not return a body when rejecting a HEAD request") {
    val stats = new NullStatsReceiver
    val service = new HttpNackFilter(stats) andThen Service.mk { _: Request => Future.exception(Failure.rejected("sadface")) }

    val request = Request()
    request.method = Method.Head

    val rep = Await.result(service(request), timeout)
    assert(rep.status == Status.ServiceUnavailable)
    assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == Some("true"))
    assert(rep.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)
    assert(rep.content.isEmpty)
  }
}
