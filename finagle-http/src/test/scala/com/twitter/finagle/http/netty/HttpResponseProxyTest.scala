package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpVersion, HttpResponseStatus}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpResponseProxyTest extends FunSuite {
  test("basics") {
    val message = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val proxy = new HttpResponseProxy {
      final val httpResponse = message
    }
    assert(proxy.httpMessage != null)
    assert(proxy.getProtocolVersion == HttpVersion.HTTP_1_1)
    assert(proxy.getStatus          == HttpResponseStatus.OK)
  }
}
