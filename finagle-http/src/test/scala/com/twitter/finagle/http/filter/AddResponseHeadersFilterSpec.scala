package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.{Await, Future}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class AddResponseHeadersFilterSpec extends SpecificationWithJUnit with Mockito {
  "AddResponseHeadersFilter" should {
    "add headers" in {
      var mocked = mock[Service[Request, Response]]
      val request = Request("/object")
      val service = new AddResponseHeadersFilter(Map("X-Money" -> "cash")) andThen mocked
      mocked(request) returns Future(request.response)
      Await.result(service(request)).headers.toMap mustEqual Map("X-Money" -> "cash")
      there was one(mocked)(request)
    }
  }
}
