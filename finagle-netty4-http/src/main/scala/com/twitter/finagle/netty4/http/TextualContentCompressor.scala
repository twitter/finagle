package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.codec.TextualContentCompressor.isTextual
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.HttpContentEncoder.Result

/**
 * Custom compressor that only handles text-like content-types with the default
 * compression level.
 */
private[http] class TextualContentCompressor extends HttpContentCompressor {

  override def beginEncode(response: HttpResponse, acceptEncoding: String): Result = {
    response.headers.get(HttpHeaderNames.CONTENT_TYPE) match {
      case ct if ct != null && isTextual(ct) => super.beginEncode(response, acceptEncoding)
      case _ => null // null return value skips compression
    }
  }
}
