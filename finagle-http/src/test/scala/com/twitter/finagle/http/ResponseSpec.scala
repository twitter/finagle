package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, DefaultHttpResponse, HttpMethod,
  HttpResponseStatus, HttpVersion}
import org.specs.SpecificationWithJUnit


class ResponseSpec extends SpecificationWithJUnit {
  "Response" should {
    "constructors" in {
      val nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

      List(Response(),
           Response(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
           Response(nettyResponse),
           Response(nettyRequest)).foreach { response =>
        response.version must_== HttpVersion.HTTP_1_1
        response.status  must_== HttpResponseStatus.OK
        response.toString mustMatch """Response\("HTTP/1.1 200 OK"\)"""
      }
    }

    "encode" in {
      val response = Response()
      response.headers("Server") = "macaw"

      val expected = "HTTP/1.1 200 OK\r\nServer: macaw\r\n\r\n"

      val actual = response.encodeString()
      actual must_== expected
    }

    "decode" in {
      val response = Response.decodeString(
        "HTTP/1.1 200 OK\r\nServer: macaw\r\nContent-Length: 0\r\n\r\n")

      response.status            must_== HttpResponseStatus.OK
      response.headers("Server") must_== "macaw"
    }
  }
}
