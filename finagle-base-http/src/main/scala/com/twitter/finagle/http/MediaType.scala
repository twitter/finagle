package com.twitter.finagle.http

object MediaType {
  // Common media types
  val Atom = "application/atom+xml"
  val Csv = "application/csv"
  val Gif = "image/gif"
  val Html = "text/html"
  val HtmlUtf8 = "text/html; charset=utf-8"
  val Iframe = "application/iframe"
  val Javascript = "application/javascript"
  val Jpeg = "image/jpeg"
  val Json = "application/json"
  val JsonUtf8 = "application/json; charset=utf-8"
  val JsonPatch = "application/json-patch+json"
  val MultipartForm = "multipart/form-data"
  val OctetStream = "application/octet-stream"
  val PlainText = "text/plain"
  val PlainTextUtf8 = "text/plain; charset=utf-8"
  val Png = "image/png"
  val Rss = "application/rss+xml"
  val Txt = "text/plain"
  val WwwForm = "application/x-www-form-urlencoded"
  val Xls = "application/vnd.ms-excel"
  val Xml = "application/xml"
  val XmlUtf8 = "application/xml; charset=utf-8"
  val Zip = "application/zip"

  /**
   * Adds a utf-8 charset parameter to a media type string.
   *
   * Note that if a charset is already set that it will add a second one,
   * it does not inspect the provided media type.
   */
  def addUtf8Charset(mediaType: String): String =
    mediaType + "; charset=utf-8"

  /**
   * Checks equality for two media types, based on their type and subtype.
   *
   * This ignores media type parameters and suffixes.
   */
  def typeEquals(left: String, right: String): Boolean = {
    val leftIndex = {
      val index = left.indexOf(';')
      if (index == -1) left.length else index
    }
    val rightIndex = {
      val index = right.indexOf(';')
      if (index == -1) right.length else index
    }
    (leftIndex == rightIndex) && left.regionMatches(0, right, 0, leftIndex)
  }
}
