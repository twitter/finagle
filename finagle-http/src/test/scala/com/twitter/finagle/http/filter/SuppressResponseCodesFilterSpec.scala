package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.http.service.NullService
import com.twitter.util.Future
import org.specs.Specification

object SuppressResponseCodesFilterSpec extends Specification {

  val dummyService = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = request.response
      request.params.get("code") match {
        case Some(code) => response.statusCode = code.toInt
        case None =>       response.status     = Status.Ok
      }
      Future.value(response)
    }
  }

  "SuppressResponseCodesFilter" should {
    "not convert 4xx to 200 if no suppress param specified" in {
      val request  = Request("code" -> "500")
      val response = SuppressResponseCodesFilter(request, dummyService)()
      response.status must_== Status.InternalServerError
    }

    "not convert 5xx to 200 if no suppress param specified" in {
      val request  = Request("code" -> "400")
      val response = SuppressResponseCodesFilter(request, dummyService)()
      response.status must_== Status.BadRequest
    }

    "convert 4xx to 200" in {
      val request  = Request("code" -> "400", "suppress_response_codes" -> "true")
      val response = SuppressResponseCodesFilter(request, dummyService)()
      response.status must_== Status.Ok
    }

    "convert 5xx to 200" in {
      val request  = Request("code" -> "500", "suppress_response_codes" -> "true")
      val response = SuppressResponseCodesFilter(request, dummyService)()
      response.status must_== Status.Ok
    }

    "ignore invalid params" in {
      val request  = Request("/search.json?q=%3G")
      val response = SuppressResponseCodesFilter(request, NullService)()
      response.status must_== Status.Ok
    }
  }
}
