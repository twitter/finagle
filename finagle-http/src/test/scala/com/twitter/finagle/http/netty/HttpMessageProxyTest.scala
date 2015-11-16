package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpMethod, HttpVersion}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpMessageProxyTest extends FunSuite {
  test("basics") {
    val message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    val proxy = new HttpMessageProxy {
      final val httpMessage = message
    }
    assert(proxy.getProtocolVersion == HttpVersion.HTTP_1_1)
  }
}
