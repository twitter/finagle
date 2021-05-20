package com.twitter.finagle.netty4.http

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class TextualContentCompressorTest extends AnyFunSuite with MockitoSugar {
  import com.twitter.finagle.http.codec.TextualContentCompressor.TextLike

  val compressor = new TextualContentCompressor
  val channel = new EmbeddedChannel(compressor)

  def newResponse(contentType: String): HttpResponse = {
    val request = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    request.headers.set(HttpHeaderNames.CONTENT_TYPE, contentType)
    request
  }

  (TextLike ++ Seq("text/plain", "text/html", "application/json;charset=utf-8")).foreach {
    contentType =>
      test("enabled for " + contentType) {
        val request = newResponse(contentType)
        val encoder = compressor.beginEncode(request, "gzip")
        assert(encoder != null)
      }
  }

  test("disabled for non-textual content-type") {
    val request = newResponse("image/gif")
    val encoder = compressor.beginEncode(request, "gzip")
    assert(encoder == null)
  }
}
