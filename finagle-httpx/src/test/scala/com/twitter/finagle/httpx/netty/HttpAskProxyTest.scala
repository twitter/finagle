package com.twitter.finagle.httpx.netty

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest=>DefaultHttpAsk, HttpRequest=>HttpAsk, _}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpAskProxyTest extends FunSuite {
  test("basics") {
    val message = new DefaultHttpAsk(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    val proxy = new HttpAskProxy {
      final val httpAsk = message
    }
    assert(proxy.httpMessage != null)
    assert(proxy.getProtocolVersion === HttpVersion.HTTP_1_1)
    assert(proxy.getMethod          === HttpMethod.GET)
  }
}
