package com.twitter.finagle.http2.transport.common

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import org.scalatest.funsuite.AnyFunSuite

class StripHeadersHandlerTest extends AnyFunSuite {
  test("removes the headers connection points to") {
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(StripHeadersHandler)
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    req.headers.add("connection", "upgrade")
    req.headers.add("upgrade", "h2c")
    channel.writeOutbound(req)
    assert(channel.readOutbound[HttpRequest].headers.isEmpty)
  }

  test("removes the te headers") {
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(StripHeadersHandler)
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    req.headers.add("te", "garbage")
    channel.writeOutbound(req)
    assert(channel.readOutbound[HttpRequest].headers.isEmpty)
  }

  test("removes the non-trailers te headers") {
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(StripHeadersHandler)
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    req.headers.add("te", "trailers")
    req.headers.add("te", "foobar")
    channel.writeOutbound(req)
    val headers = channel.readOutbound[HttpRequest].headers
    assert(headers.size == 1)
    assert(headers.get("te") == "trailers")
  }

  test("removes the http2-settings headers") {
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(StripHeadersHandler)
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    req.headers.add("http2-settings", "garbage")
    channel.writeOutbound(req)
    assert(channel.readOutbound[HttpRequest].headers.isEmpty)
  }

  test("removes stuff for objects in the opposite direction") {
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(StripHeadersHandler)
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    req.headers.add("http2-settings", "garbage")
    channel.writeInbound(req)
    assert(channel.readInbound[HttpRequest].headers.isEmpty)
  }
}
