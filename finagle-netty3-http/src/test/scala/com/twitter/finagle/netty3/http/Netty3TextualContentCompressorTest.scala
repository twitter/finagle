package com.twitter.finagle.netty3.http

import org.jboss.netty.handler.codec.http._
import org.scalatest.FunSuite

class Netty3TextualContentCompressorTest extends FunSuite {
  import com.twitter.finagle.http.codec.TextualContentCompressor.TextLike

  private val compressor = new Netty3TextualContentCompressor

  def newRequest(contentType: String): HttpMessage = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    request.headers.set(HttpHeaders.Names.CONTENT_TYPE, contentType)
    request
  }

  (TextLike ++ Seq("text/plain", "text/html", "application/json;charset=utf-8")) foreach {
    contentType =>
      test("enabled for " + contentType) {
        val request = newRequest(contentType)
        val encoder = compressor.newContentEncoder(request, "gzip")
        assert(encoder != null)
      }
  }

  test("disabled for non-textual content-type") {
    val request = newRequest("image/gif")
    val encoder = compressor.newContentEncoder(request, "gzip")
    assert(encoder == null)
  }
}
