package com.twitter.finagle.http2.transport.client

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{ChannelHandlerContext, ChannelOutboundHandlerAdapter, ChannelPromise}
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2.{Http2Error, Http2Exception}
import org.scalatest.funsuite.AnyFunSuite

class Http2ClientEventMapperTest extends AnyFunSuite {
  private class Ctx {
    val ec = new EmbeddedChannel(Http2ClientEventMapper)
  }

  test("HeaderListSizeException on HttpRequest write spoofs a 431 response and closes") {
    val ctx = new Ctx
    import ctx._

    ec.pipeline.addFirst(new ChannelOutboundHandlerAdapter {
      override def write(
        ctx: ChannelHandlerContext,
        msg: scala.Any,
        promise: ChannelPromise
      ): Unit = {
        promise.setFailure(Http2Exception.headerListSizeError(1, Http2Error.CANCEL, true, ""))
      }
    })

    val promise =
      ec.writeOneOutbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))

    val response = ec.readInbound[FullHttpResponse]()

    assert(promise.isSuccess)
    assert(response.status == HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE)
    assert(response.isInstanceOf[LastHttpContent])
    assert(!ec.isOpen)
  }
}
