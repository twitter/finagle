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
      }
    }
  }
}
