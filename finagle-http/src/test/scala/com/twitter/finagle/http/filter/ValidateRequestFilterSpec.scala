package com.twitter.finagle.http.filter

import com.twitter.finagle.http.{Request, Status}
import com.twitter.finagle.http.service.NullService
import org.specs.Specification

object ValidateRequestFilterSpec extends Specification {

  "ValidateRequestFilter" should {
    "ignore ok path and params" in {
      val request  = Request("/")
      val response = ValidateRequestFilter(request, NullService)()
      response.status must_== Status.Ok
    }

    "error on bad request" in {
      val request = Request("/bad-http-request")
      val response = ValidateRequestFilter(request, NullService)()
      response.status must_== Status.BadRequest
    }

    "errror on invalid params" in {
      val request  = Request("/search.json?q=%3G")
      val response = ValidateRequestFilter(request, NullService)()
      response.status must_== Status.BadRequest
    }
  }
}
