package com.twitter.finagle.http.filter

import com.twitter.finagle.http.{Ask, Status}
import com.twitter.finagle.http.service.NullService
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ValidateAskFilterTest extends FunSuite {

  test("ignore ok path and params") {
    val request  = Ask("/")
    val response = Await.result(ValidateAskFilter(request, NullService))
    assert(response.status === Status.Ok)
  }

  test("error on bad request") {
    val request = Ask("/bad-http-request")
    val response = Await.result(ValidateAskFilter(request, NullService))
    assert(response.status === Status.BadAsk)
  }

  test("errror on invalid params") {
    val request  = Ask("/search.json?q=%3G")
    val response = Await.result(ValidateAskFilter(request, NullService))
    assert(response.status === Status.BadAsk)
  }
}
