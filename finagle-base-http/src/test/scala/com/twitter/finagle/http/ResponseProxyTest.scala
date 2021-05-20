package com.twitter.finagle.http

import org.scalatest.funsuite.AnyFunSuite

class ResponseProxyTest extends AnyFunSuite {
  test("response.ctx") {
    val field = Response.Schema.newField[Int]
    val resp = Response()
    resp.ctx(field) = 42
    val proxyResp = new ResponseProxy {
      def response = resp
    }

    assert(proxyResp.ctx(field) == 42)
  }

  test("response.cookies") {
    val cookie = new Cookie("foo", "bar")
    val resp = Response()
    val proxy = new Response.Proxy {
      def response = resp
    }

    assert(proxy.cookies.get("foo") == None) // to initialize the map

    resp.cookies += cookie

    assert(proxy.cookies.get("foo") == Some(cookie))
  }
}
