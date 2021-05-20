package com.twitter.finagle.http

import com.twitter.finagle.http
import org.scalatest.funsuite.AnyFunSuite

class RequestProxyTest extends AnyFunSuite {
  test("request.ctx") {
    val field = Request.Schema.newField[Int]
    val request1 = Request()
    request1.ctx(field) = 42
    val request2 = new RequestProxy {
      def request = request1
    }

    assert(request2.ctx(field) == 42)
  }

  test("request.cookies") {
    val cookie = new Cookie("foo", "bar")
    val req = Request()
    val proxy = new http.Request.Proxy {
      def request: Request = req
    }

    assert(proxy.cookies.get("foo") == None) // to initialize the map

    req.cookies += cookie

    assert(proxy.cookies.get("foo") == Some(cookie))
  }

  test("request uri params") {
    val req = Request("/search?q=hello")
    val proxy = new http.Request.Proxy {
      def request: Request = req
    }

    assert(proxy.params.get("q") == Some("hello"))
  }

  test("request body params") {
    val req = Request(Method.Post, "/search")
    req.contentType = "application/x-www-form-urlencoded"
    req.contentString = "q=twitter"
    val proxy = new http.Request.Proxy {
      def request: Request = req
    }

    assert(proxy.params.get("q") == Some("twitter"))
  }

}
