package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.Future
import org.specs.Specification
import org.specs.mock.Mockito

object AddResponseHeadersFilterSpec extends Specification with Mockito {
  "AddResponseHeadersFilter" should {
    "add headers" in {
      var mocked = mock[Service[Request, Response]]
      val request = Request("/object")
      val service = new AddResponseHeadersFilter(Map("X-Money" -> "cash")) andThen mocked
      mocked(request) returns Future(request.response)
      service(request).apply().headers.toMap mustEqual Map("X-Money" -> "cash")
      there was one(mocked)(request)
    }
  }
}
