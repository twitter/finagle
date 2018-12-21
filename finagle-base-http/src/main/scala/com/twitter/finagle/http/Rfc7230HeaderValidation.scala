package com.twitter.finagle.http

import java.util.BitSet

/**
 * Validation methods for HTTP headers.
 *
 * Methods that provide RFC-7230 (https://tools.ietf.org/html/rfc7230) header
 * validation. Invalid names or values will result in throwing an
 * `IllegalArgumentException`.
 */
private object Rfc7230HeaderValidation {

  private[this] def validHeaderNameChars: Iterable[Int] = {
    // https://tools.ietf.org/html/rfc7230#section-3.2.6
    //
    // token          = 1*tchar
    //
    // tchar          = "!" / "#" / "$" / "%" / "&" / "'" / "*"
    //                / "+" / "-" / "." / "^" / "_" / "`" / "|" / "~"
    //                / DIGIT / ALPHA
    //                ; any VCHAR, except delimiters
    ("!#$%&'*+-.^_`|~" ++
      ('0' to '9') ++ // DIGIT
      ('a' to 'z') ++ ('A' to 'Z')).map(_.toInt) // ALPHA
  }

  private[this] def validHeaderValueChars: Iterable[Int] = {
    // Header fields: https://tools.ietf.org/html/rfc7230#section-3.2
    //
    // field-value    = *( field-content / obs-fold )
    // field-content  = field-vchar [ 1*( SP / HTAB ) field-vchar ]
    // field-vchar    = VCHAR / obs-text
    // obs-fold       = CRLF 1*( SP / HTAB )
    // VCHAR          =  %x21-7E
    //     ; visible (printing) characters, https://tools.ietf.org/html/rfc5234#appendix-B.1
    // obs-text       = %x80-FF; https://tools.ietf.org/html/rfc7230#section-3.2.6

    (0x21 to 0x7e) ++ // VCHAR
      (0x80 to 0xff) ++ // obs-text
      "\r\n \t".map(_.toInt) // Valid whitespace and obs-fold chars
  }

  private[this] val validHeaderNameCharSet: java.util.BitSet = toBitSet(validHeaderNameChars)

  private[this] val validHeaderValueCharSet: java.util.BitSet = toBitSet(validHeaderValueChars)

  private[this] def toBitSet(valid: Iterable[Int]): BitSet = {
    val bitSet = new BitSet
    valid.foreach(bitSet.set)
    bitSet
  }

  private[this] def validHeaderNameChar(c: Char): Boolean = validHeaderNameCharSet.get(c)

  private[this] def validHeaderValueChar(c: Char): Boolean = validHeaderValueCharSet.get(c)

  /**
   * Validate the provided header name.
   * @param name the header name to be validated.
   * @throws IllegalArgumentException if the header name is not compliant.
   */
  def validateName(name: CharSequence): Unit = {
    if (name == null) throw new NullPointerException("Header names cannot be null")
    if (name.length == 0) throw new IllegalArgumentException("Header name cannot be empty")

    var i = 0
    while (i < name.length) {
      val c = name.charAt(i)
      if (!validHeaderNameChar(c))
        throw new IllegalArgumentException(
          s"Header '$name': name cannot contain the prohibited character '0x${Integer.toHexString(c)}': " + c
        )

      i += 1
    }
  }

  private[this] sealed trait ObsFoldState
  private[this] case object NonFold extends ObsFoldState
  private[this] case object LF extends ObsFoldState
  private[this] case object CR extends ObsFoldState

  /**
   * Validate the header value.
   *
   * @param name the header name. Only used for exception messages and is not validated.
   * @param value the header value to be validated.
   * @return true if the header value contained an obs-fold sequence, false otherwise.
   * @throws IllegalArgumentException if the header value is not compliant.
   */
  def validateValue(name: CharSequence, value: CharSequence): Boolean = {
    if (value == null) throw new NullPointerException("Header values cannot be null")

    var i = 0
    // NonFold: Previous character was neither CR nor LF
    // CR: The previous character was CR
    // LF: The previous character was LF
    var state: ObsFoldState = NonFold
    var foldDetected = false

    while (i < value.length) {
      val c = value.charAt(i)

      if (!validHeaderValueChar(c))
        throw new IllegalArgumentException(
          s"Header '$name': value contains a prohibited character '0x${Integer.toHexString(c)}': $c"
        )

      state match {
        case NonFold =>
          if (c == '\r') state = CR
          else if (c == '\n') state = LF
        case CR =>
          if (c == '\n') state = LF
          else
            throw new IllegalArgumentException(
              s"Header '$name': only '\\n' is allowed after '\\r' in value")
        case LF =>
          if (c == '\t' || c == ' ') {
            foldDetected = true
            state = NonFold
          } else
            throw new IllegalArgumentException(
              s"Header '$name': only ' ' and '\\t' are allowed after '\\n' in value")
      }

      i += 1
    }

    if (state != NonFold) {
      throw new IllegalArgumentException(
        s"Header '$name': value must not end with '\\r' or '\\n'. Observed: " +
          (if (state == CR) "\\r" else "\\n")
      )
    }
    foldDetected
  }
}
