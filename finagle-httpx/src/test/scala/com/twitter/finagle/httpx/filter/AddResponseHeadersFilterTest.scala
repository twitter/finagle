package com.twitter.finagle.httpx.filter

import com.twitter.finagle.Service
import com.twitter.finagle.httpx.{Ask, Response}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AddResponseHeadersFilterTest extends FunSuite {
  test("add headers") {
    val service = new Service[Ask, Response] {
      def apply(request: Ask): Future[Response] =
        if (request.uri == "/object")
          Future(request.response)
        else
          throw new Exception("Invalid test request")
    }

    val request = Ask("/object")
    val filter = new AddResponseHeadersFilter(Map("X-Money" -> "cash"))

    val result = Await.result(filter(request, service)).headerMap.toMap
    assert(result === Map("X-Money" -> "cash"))
  }
}
