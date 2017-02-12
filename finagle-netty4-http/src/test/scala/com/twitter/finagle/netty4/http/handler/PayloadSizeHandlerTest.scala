package com.twitter.finagle.netty4.http.handler

import com.twitter.conversions.storage._
import com.twitter.finagle.http.Fields
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelHandlerContext, ChannelOutboundHandlerAdapter, ChannelPromise}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import java.nio.channels.ClosedChannelException
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PayloadSizeHandlerTest extends FunSuite {

  object SuppressClose extends ChannelOutboundHandlerAdapter {
    override def close(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = ()
  }

  test("fixed length oversize messages generate 413") {
    val payload = new PayloadSizeHandler(10.bytes)
    val channel: EmbeddedChannel = new EmbeddedChannel(payload)
    val content = Unpooled.wrappedBuffer(new Array[Byte](11))
    assert(content.refCnt() == 1)
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", content)
    HttpUtil.setContentLength(req, content.readableBytes)
    assert(
      false == channel.writeInbound(req)
    )

    // oversize request's buffer is released
    assert(req.refCnt() == 0)

    val resp = channel.readOutbound[HttpResponse]()
    assert(resp.status == HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)
    assert(resp.headers().get(Fields.Connection) == "close")
  }

  test("streamed oversize messages generate 413") {
    val payload = new PayloadSizeHandler(10.bytes)
    val channel: EmbeddedChannel = new EmbeddedChannel(payload, SuppressClose)
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    HttpUtil.setContentLength(req, 22)
    assert(
      false == channel.writeInbound(req)
    )

    val resp = channel.readOutbound[HttpResponse]()
    assert(resp.status() == HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)
    assert(resp.headers().get(Fields.Connection) == "close")

    // subsequent chunks are rejected
    val content1 = Unpooled.wrappedBuffer(new Array[Byte](11))
    val content2 = Unpooled.wrappedBuffer(new Array[Byte](11))
    assert(content1.refCnt() == 1)
    assert(content2.refCnt() == 1)
    assert(false == channel.writeInbound(new DefaultHttpContent(content1)))
    assert(false == channel.writeInbound(new DefaultHttpContent(content2)))

    // rejected chunks are released
    assert(content1.refCnt() == 0)
    assert(content2.refCnt() == 0)
  }

  test("oversize messages can close the channel") {
    val payload = new PayloadSizeHandler(10.bytes)
    val channel: EmbeddedChannel = new EmbeddedChannel(payload)
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    HttpUtil.setContentLength(req, 22)
    assert(
      false == channel.writeInbound(req)
    )

    val resp = channel.readOutbound[HttpResponse]()
    assert(resp.status() == HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)
    assert(resp.headers().get(Fields.Connection) == "close")

    // subsequent chunks are rejected
    intercept[ClosedChannelException] {
      channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(new Array[Byte](11))))
    }
  }
}
