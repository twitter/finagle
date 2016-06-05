package com.twitter.finagle.http.filter

import com.twitter.finagle.http.{Request, Status}
import com.twitter.finagle.http.service.NullService
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ValidateRequestFilterTest extends FunSuite {

  test("ignore ok path and params") {
    val request  = Request("/")
    val response = Await.result(ValidateRequestFilter(request, NullService))
    assert(response.status == Status.Ok)
  }

  test("error on bad request") {
    val request = Request("/bad-http-request")
    val response = Await.result(ValidateRequestFilter(request, NullService))
    assert(response.status == Status.BadRequest)
  }

  test("errror on invalid params") {
    val request  = Request("/search.json?q=%3G")
    val response = Await.result(ValidateRequestFilter(request, NullService))
    assert(response.status == Status.BadRequest)
  }
}
