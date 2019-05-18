package com.twitter.finagle.netty4.http.util

private[finagle] object UriUtils {

  case class InvalidUriException(uri: CharSequence) extends Exception(s"Invalid URI found: $uri")

  def isValidUri(uri: CharSequence): Boolean = isValidUri(uri, uri.length())

  private[this] def isValidUri(uri: CharSequence, size: Int): Boolean = {
    if (size == 0 || uri == "/") return true

    var idx: Int = 0
    var ch: Char = uri.charAt(idx)

    // ex: "/1234f.jpg?query=123" should validate everything before ?query=123

    while (idx <= size && ch != '?') { //the query string param will get encoded/decoded by Netty
      if (isAscii(ch) && !ch.isWhitespace) {
        idx += 1
        if (idx < size) ch = uri.charAt(idx)
      } else {
        return false
      }
    }

    //otherwise, we're all good
    true
  }

  private[this] def isAscii(ch: Char): Boolean = ch < 128

}
