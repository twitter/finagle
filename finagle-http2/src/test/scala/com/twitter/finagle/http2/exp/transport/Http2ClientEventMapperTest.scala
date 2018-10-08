package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.http.{Fields, TooLongMessageException}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.{DefaultHttpResponse, HttpResponse, HttpResponseStatus, HttpVersion}
import io.netty.handler.codec.http2.{Http2Error, Http2Exception}
import org.scalatest.FunSuite

class Http2ClientEventMapperTest extends FunSuite {
  private class Ctx {
    val ec = new EmbeddedChannel(Http2ClientEventMapper)
  }

  test("HeaderListSizeException is converted to a TooLongFrameException") {
    val ctx = new Ctx
    import ctx._

    ec.pipeline.fireExceptionCaught(
      Http2Exception.headerListSizeError(1, Http2Error.CANCEL, true, ""))

    intercept[TooLongMessageException] { ec.checkException() }
  }

  test("Adds a 'Connection: close' header to HttpResponse objects") {
    val ctx = new Ctx
    import ctx._

    val msg = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    ec.writeInbound(msg)
    val response = ec.readInbound[HttpResponse]()

    assert(msg eq response)
    assert(response.headers.get(Fields.Connection) == "close")
  }
}
