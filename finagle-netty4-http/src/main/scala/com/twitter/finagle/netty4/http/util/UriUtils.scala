package com.twitter.finagle.netty4.http.util

private[finagle] object UriUtils {

  /**
   * @return true if the uri is valid or null, false otherwise
   */
  def isValidUri(uri: CharSequence): Boolean =
    if (uri == null) true else isValidUri(uri, uri.length())

  private[this] def isValidUri(uri: CharSequence, size: Int): Boolean = {
    if (size == 0 || uri == "/") return true

    var idx: Int = 0
    var ch: Char = uri.charAt(idx)

    // ex: "/1234f.jpg?query=123" should validate everything before ?query=123

    while (idx < size && ch != '?') { //the query string param will get encoded/decoded by Netty
      if (isAscii(ch) && !ch.isWhitespace) {
        idx += 1
        if (ch == '%') {
          if (!isValidEncoding(uri, size, idx)) return false
          idx += 2
        }

        if (idx < size) ch = uri.charAt(idx)
      } else {
        return false
      }
    }

    //otherwise, we're all good
    true
  }

  private[this] def isValidEncoding(uri: CharSequence, size: Int, offset: Int): Boolean =
    (offset + 1 < size) && isHex(uri.charAt(offset)) && isHex(uri.charAt(offset + 1))

  private[this] def isHex(ch: Char): Boolean = Character.digit(ch, 16) >= 0

  private[this] def isAscii(ch: Char): Boolean = ch < 128

}
