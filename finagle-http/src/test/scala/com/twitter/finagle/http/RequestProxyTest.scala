package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RequestProxyTest extends FunSuite {
  test("request.ctx") {
    val field = Request.Schema.newField[Int]
    val request1 = Request()
    request1.ctx(field) = 42
    val request2 = new RequestProxy {
      override val request = request1
    }
    assert(request2.ctx(field) == 42)
  }
}
