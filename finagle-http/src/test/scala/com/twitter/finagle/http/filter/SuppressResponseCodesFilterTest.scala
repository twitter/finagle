package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.service.NullService
import com.twitter.finagle.http.{Ask, Response, Status}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SuppressResponseCodesFilterTest extends FunSuite {

  val dummyService = new Service[Ask, Response] {
    def apply(request: Ask): Future[Response] = {
      val response = request.response
      request.params.get("code") match {
        case Some(code) => response.statusCode = code.toInt
        case None =>       response.status     = Status.Ok
      }
      Future.value(response)
    }
  }

  test("not convert 5xx to 200 if no suppress param specified") {
    val request  = Ask("code" -> "500")
    val response = Await.result(SuppressResponseCodesFilter(request, dummyService))
    assert(response.status === Status.InternalServerError)
  }

  test("not convert 4xx to 200 if no suppress param specified") {
    val request  = Ask("code" -> "400")
    val response = Await.result(SuppressResponseCodesFilter(request, dummyService))
    assert(response.status === Status.BadAsk)
  }

  test("convert 4xx to 200") {
    val request  = Ask("code" -> "400", "suppress_response_codes" -> "true")
    val response = Await.result(SuppressResponseCodesFilter(request, dummyService))
    assert(response.status === Status.Ok)
  }

  test("convert 5xx to 200") {
    val request  = Ask("code" -> "500", "suppress_response_codes" -> "true")
    val response = Await.result(SuppressResponseCodesFilter(request, dummyService))
    assert(response.status === Status.Ok)
  }

  test("ignore invalid params") {
    val request  = Ask("/search.json?q=%3G")
    val response = Await.result(SuppressResponseCodesFilter(request, NullService))
    assert(response.status === Status.Ok)
  }
}
