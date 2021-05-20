package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class AddResponseHeadersFilterTest extends AnyFunSuite {
  test("add headers") {
    val service = new Service[Request, Response] {
      def apply(request: Request): Future[Response] =
        if (request.uri == "/object")
          Future(Response())
        else
          throw new Exception("Invalid test request")
    }

    val request = Request("/object")
    val filter = new AddResponseHeadersFilter(Map("X-Money" -> "cash"))

    val result = Await.result(filter(request, service), 1.second).headerMap.toMap
    assert(result == Map("X-Money" -> "cash"))
  }
}
