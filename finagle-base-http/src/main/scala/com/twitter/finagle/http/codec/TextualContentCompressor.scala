package com.twitter.finagle.http.codec

private[finagle] object TextualContentCompressor {
  def isTextual(contentType: String): Boolean = {
    val contentTypeWithoutCharset = contentType.split(";", 2) match {
      case Array(charsetContentType, _) => charsetContentType
      case _ => contentType
    }
    val lowerCased = contentTypeWithoutCharset.toLowerCase.trim()
    lowerCased.startsWith("text/") || TextLike.contains(lowerCased)
  }

  val TextLike = Set(
    "image/svg+xml",
    "application/atom+xml",
    "application/javascript",
    "application/json",
    "application/rss+xml",
    "application/x-javascript",
    "application/xhtml+xml",
    "application/xml"
  )
}
