package com.twitter.finagle.netty4.http

import io.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TextualContentCompressorTest extends FunSuite {
  import com.twitter.finagle.http.codec.TextualContentCompressor.TextLike

  val compressor = new TextualContentCompressor

  def newResponse(contentType: String): HttpResponse = {
    val request = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    request.headers.set(HttpHeaderNames.CONTENT_TYPE, contentType)
    request
  }

  (TextLike ++ Seq("text/plain", "text/html", "application/json;charset=utf-8")).foreach { contentType =>
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
