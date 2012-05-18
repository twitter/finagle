package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpVersion, HttpResponseStatus}
import org.specs.SpecificationWithJUnit


class HttpResponseProxySpec extends SpecificationWithJUnit {
  "HttpRequestProxy" should {
    "basics" in {
      val message = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      val proxy = new HttpResponseProxy {
        final val httpResponse = message
      }
      proxy.httpMessage        must notBeNull
      proxy.getProtocolVersion must_== HttpVersion.HTTP_1_1
      proxy.getStatus          must_== HttpResponseStatus.OK
    }
  }
}
