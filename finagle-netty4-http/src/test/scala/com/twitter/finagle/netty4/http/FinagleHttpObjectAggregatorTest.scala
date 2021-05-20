package com.twitter.finagle.netty4.http

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.HttpHeaderNames.EXPECT
import io.netty.handler.codec.http.HttpHeaderValues.CONTINUE
import org.scalatest.funsuite.AnyFunSuite

class FinagleHttpObjectAggregatorTest extends AnyFunSuite {
  def headers = {
    val res = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    res.headers.set(EXPECT, CONTINUE)
    res
  }
  def last = new DefaultLastHttpContent()

  test("can send automatic 100-CONTINUE responses") {
    val e = new EmbeddedChannel(new FinagleHttpObjectAggregator(1024, handleExpectContinue = true))
    e.writeInbound(headers)
    e.writeInbound(last)
    val resp = e.readOutbound[HttpResponse]()
    assert(resp.status() == HttpResponseStatus.CONTINUE)
  }

  test("can suppress automatic 100-CONTINUE responses") {
    val e = new EmbeddedChannel(new FinagleHttpObjectAggregator(1024, handleExpectContinue = false))
    e.writeInbound(headers)
    e.writeInbound(last)
    assert(e.outboundMessages().isEmpty)
  }
}
