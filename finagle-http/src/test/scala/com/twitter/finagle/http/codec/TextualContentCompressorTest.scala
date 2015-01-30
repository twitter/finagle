package com.twitter.finagle.http.codec

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest=>DefaultHttpAsk, _}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TextualContentCompressorTest extends FunSuite {
  import TextualContentCompressor.TextLike

  val compressor = new TextualContentCompressor

  def newAsk(contentType: String): HttpMessage = {
    val request = new DefaultHttpAsk(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    request.headers.set(HttpHeaders.Names.CONTENT_TYPE, contentType)
    request
  }

  (TextLike ++ Seq("text/plain", "text/html", "application/json;charset=utf-8")) foreach { contentType =>
    test("enabled for " + contentType) {
      val request = newAsk(contentType)
      val encoder = compressor.newContentEncoder(request, "gzip")
      assert(encoder != null)
    }
  }

  test("disabled for non-textual content-type") {
    val request = newAsk("image/gif")
    val encoder = compressor.newContentEncoder(request, "gzip")
    assert(encoder == null)
  }
}
