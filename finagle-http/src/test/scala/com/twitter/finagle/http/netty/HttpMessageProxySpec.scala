package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpMethod, HttpVersion}
import org.specs.SpecificationWithJUnit


class HttpMessageProxySpec extends SpecificationWithJUnit {
  "HttpMessageProxy" should {
    "basics" in {
      val message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      val proxy = new HttpMessageProxy {
        final val httpMessage = message
      }
      proxy.getProtocolVersion must_== HttpVersion.HTTP_1_1
    }
  }
}
