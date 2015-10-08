package com.twitter.finagle.http.codec

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.embedder.EncoderEmbedder
import org.jboss.netty.handler.codec.http.{HttpContentCompressor, HttpHeaders, HttpMessage}

/**
 * Custom compressor that only handles text-like content-types with the default
 * compression level.
 */
private[http]
class TextualContentCompressor extends HttpContentCompressor {
  import TextualContentCompressor._

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

  private[this] def isTextual(contentType: String) = {
    val contentTypeWithoutCharset = contentType.split(";", 2) match {
      case Array(charsetContentType, _) => charsetContentType
      case _ => contentType
    }
    val lowerCased = contentTypeWithoutCharset.toLowerCase.trim()
    lowerCased.startsWith("text/") || TextLike.contains(lowerCased)
  }
}

private object TextualContentCompressor {
  val TextLike = Set(
    "image/svg+xml",
    "application/atom+xml",
    "application/javascript",
    "application/json",
    "application/rss+xml",
    "application/x-javascript",
    "application/xhtml+xml",
    "application/xml")
}
