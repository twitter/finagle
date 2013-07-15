package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpMethod, HttpVersion}
import org.specs.SpecificationWithJUnit


class HttpRequestProxySpec extends SpecificationWithJUnit {
  "HttpRequestProxy" should {
    "basics" in {
      val message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      val proxy = new HttpRequestProxy {
        final val httpRequest = message
      }
      proxy.httpMessage        must notBeNull
      proxy.getProtocolVersion must_== HttpVersion.HTTP_1_1
      proxy.getMethod          must_== HttpMethod.GET
    }
  }
}
