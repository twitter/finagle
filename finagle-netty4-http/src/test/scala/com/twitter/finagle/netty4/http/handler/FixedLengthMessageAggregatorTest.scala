package com.twitter.finagle.netty4.http.handler

import com.twitter.conversions.storage._
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FixedLengthMessageAggregatorTest extends FunSuite {

  test("full messages pass through") {
    val agg = new FixedLengthMessageAggregator(10.megabytes)
    val channel: EmbeddedChannel = new EmbeddedChannel(new HttpRequestEncoder(), agg)
    val content = Unpooled.wrappedBuffer(new Array[Byte](11))
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", content)
    assert(channel.writeInbound(req))

    val reqObserved = channel.readInbound[FullHttpRequest]()
    assert(reqObserved.method == HttpMethod.POST)
    assert(reqObserved.content == req.content)
  }

  test("chunked messages aren't aggregated") {
    val agg = new FixedLengthMessageAggregator(10.megabytes)
    val channel: EmbeddedChannel = new EmbeddedChannel(new HttpRequestEncoder(), agg)
    val content = Unpooled.wrappedBuffer(new Array[Byte](11))
    val head = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    HttpUtil.setTransferEncodingChunked(head, true)

    val body = new DefaultLastHttpContent(content)
    assert(channel.writeInbound(head))
    assert(channel.writeInbound(body))

    val reqObserved = channel.readInbound[HttpRequest]()
    assert(reqObserved.method == HttpMethod.POST)

    val bodyObserved = channel.readInbound[HttpContent]()
    assert(bodyObserved.content == content)
  }

  test("fixed length messages which are chunked and smaller than " +
       "the specified length are aggregated") {
    val agg = new FixedLengthMessageAggregator(12.bytes)
    val channel: EmbeddedChannel = new EmbeddedChannel(new HttpRequestEncoder(), agg)
    val content = Unpooled.wrappedBuffer(new Array[Byte](11))
    val head = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    HttpUtil.setContentLength(head, content.readableBytes)

    val body = new DefaultLastHttpContent(content)

    assert(!channel.writeInbound(head))
    assert(channel.writeInbound(body))

    val reqObserved = channel.readInbound[FullHttpRequest]()
    assert(reqObserved.method == HttpMethod.POST)
    assert(reqObserved.content == content)
  }

  test("fixed length messages which are chunked and equal to the specified length are aggregated") {
    val agg = new FixedLengthMessageAggregator(11.bytes)
    val channel: EmbeddedChannel = new EmbeddedChannel(new HttpRequestEncoder(), agg)
    val content = Unpooled.wrappedBuffer(new Array[Byte](11))
    val head = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    HttpUtil.setContentLength(head, content.readableBytes)

    val body = new DefaultLastHttpContent(content)

    assert(!channel.writeInbound(head))
    assert(channel.writeInbound(body))

    val reqObserved = channel.readInbound[FullHttpRequest]()
    assert(reqObserved.method == HttpMethod.POST)
    assert(reqObserved.content == content)
  }

  test("fixed length messages which are chunked and larger than than the " +
       "specified size remain chunked") {
    val agg = new FixedLengthMessageAggregator(11.byte)
    val channel: EmbeddedChannel = new EmbeddedChannel(new HttpRequestEncoder(), agg)
    val content = Unpooled.wrappedBuffer(new Array[Byte](12))
    val head = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    HttpUtil.setContentLength(head, content.readableBytes)

    val body = new DefaultLastHttpContent(content)

    assert(channel.writeInbound(head))
    assert(channel.writeInbound(body))

    val reqObserved = channel.readInbound[HttpRequest]()
    assert(reqObserved.method == HttpMethod.POST)

    val bodyObserved = channel.readInbound[HttpContent]()
    assert(bodyObserved.content == content)
  }
}
