package com.twitter.finagle.netty3.http

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.embedder.EncoderEmbedder
import org.jboss.netty.handler.codec.http.{HttpContentCompressor, HttpHeaders, HttpMessage}

/**
 * Custom compressor that only handles text-like content-types with the default
 * compression level.
 */
private class Netty3TextualContentCompressor extends HttpContentCompressor {
  import com.twitter.finagle.http.codec.TextualContentCompressor._

  override def newContentEncoder(msg: HttpMessage, acceptEncoding: String) =
    contentEncoder(msg, acceptEncoding).orNull

  private[this] def contentEncoder(
    msg: HttpMessage,
    acceptEncoding: String
  ): Option[EncoderEmbedder[ChannelBuffer]] = {
    Option(msg.headers.get(HttpHeaders.Names.CONTENT_TYPE)) collect {
      case ctype if isTextual(ctype) =>
        super.newContentEncoder(msg, acceptEncoding)
    }
  }

}
