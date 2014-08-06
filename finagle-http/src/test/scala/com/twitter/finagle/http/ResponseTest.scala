package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, DefaultHttpResponse, HttpMethod,
  HttpResponseStatus, HttpVersion}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResponseTest extends FunSuite {
  test("constructors") {
    val nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

    List(Response(),
      Response(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
      Response(nettyResponse),
      Response(nettyRequest)).foreach { response =>
      assert(response.version === HttpVersion.HTTP_1_1)
      assert(response.status === HttpResponseStatus.OK)
      assert("""Response\("HTTP/1.1 200 OK"\)""".r.findFirstIn(response.toString) === Some(response.toString))
    }
  }

  test("encode") {
    val response = Response()
    response.headers.set("Server", "macaw")

    val expected = "HTTP/1.1 200 OK\r\nServer: macaw\r\n\r\n"
    val actual = response.encodeString()

    assert(actual === expected)
  }

  test("decode") {
    val response = Response.decodeString(
      "HTTP/1.1 200 OK\r\nServer: macaw\r\nContent-Length: 0\r\n\r\n")

    assert(response.status === HttpResponseStatus.OK)
    assert(response.headers.get("Server") === "macaw")
  }
}
