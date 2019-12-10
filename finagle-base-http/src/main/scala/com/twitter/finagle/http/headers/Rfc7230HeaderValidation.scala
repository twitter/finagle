package com.twitter.finagle.http.headers

import java.util.BitSet

/**
 * Validation methods for HTTP headers.
 *
 * Methods that provide RFC-7230 (https://tools.ietf.org/html/rfc7230) header
 * validation. Invalid names or values will result in throwing an
 * `HeaderValidationException`.
 */
object Rfc7230HeaderValidation {

  /** Exception that represents header validation failure */
  sealed abstract class HeaderValidationException(details: String)
      extends IllegalArgumentException(details)

  /** Invalid header name */
  final class NameValidationException(details: String) extends HeaderValidationException(details)

  /** Invalid header value */
  final class ValueValidationException(details: String) extends HeaderValidationException(details)

  /** Validation result for header names */
  sealed trait NameValidationResult

  /** Validation result for header values */
  sealed trait ValueValidationResult

  /** Successful validation */
  case object ValidationSuccess extends NameValidationResult with ValueValidationResult

  /** Successful value validation with the detection of an obs-fold sequence */
  case object ObsFoldDetected extends ValueValidationResult

  /** Validation failed with the provided cause */
  case class ValidationFailure(ex: HeaderValidationException)
      extends NameValidationResult
      with ValueValidationResult

  private[this] def validHeaderNameChars: Iterable[Int] = {
    // https://tools.ietf.org/html/rfc7230#section-3.2.6
    //
    // token          = 1*tchar
    //
    // tchar          = "!" / "#" / "$" / "%" / "&" / "'" / "*"
    //                / "+" / "-" / "." / "^" / "_" / "`" / "|" / "~"
    //                / DIGIT / ALPHA
    //                ; any VCHAR, except delimiters
    ("!#$%&'*+-.^_`|~".toList ++
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

  private[this] val ObsFoldRegex = "\r?\n[\t ]+".r

  private[this] val validHeaderNameCharSet: java.util.BitSet = toBitSet(validHeaderNameChars)

  private[this] val validHeaderValueCharSet: java.util.BitSet = toBitSet(validHeaderValueChars)

  private[this] def toBitSet(valid: Iterable[Int]): BitSet = {
    val bitSet = new BitSet
    valid.foreach(bitSet.set)
    bitSet
  }

  private[this] def validHeaderNameChar(c: Char): Boolean = validHeaderNameCharSet.get(c)

  private[this] def validHeaderValueChar(c: Char): Boolean = validHeaderValueCharSet.get(c)

  /** Replace obs-fold sequences in the value with whitespace */
  def replaceObsFold(value: CharSequence): String = ObsFoldRegex.replaceAllIn(value, " ")

  /**
   * Validate the provided header name.
   * @param name the header name to be validated.
   */
  def validateName(name: CharSequence): NameValidationResult = {
    if (name == null) throw new NullPointerException("Header names cannot be null")
    else if (name.length == 0)
      ValidationFailure(new NameValidationException("Header name cannot be empty"))
    else {
      var i = 0
      while (i < name.length) {
        val c = name.charAt(i)
        if (!validHeaderNameChar(c))
          return ValidationFailure(
            new NameValidationException(
              s"Header '$name': name cannot contain the prohibited character '0x${Integer
                .toHexString(c)}': " + c
            ))
        i += 1
      }
      // If we made it here everything was fine.
      ValidationSuccess
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
   */
  def validateValue(name: CharSequence, value: CharSequence): ValueValidationResult = {
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
        return ValidationFailure(new ValueValidationException(
          s"Header '$name': value contains a prohibited character '0x${Integer.toHexString(c)}': $c"
        ))

      state match {
        case NonFold =>
          if (c == '\r') state = CR
          else if (c == '\n') state = LF
        case CR =>
          if (c == '\n') state = LF
          else
            return ValidationFailure(
              new ValueValidationException(
                s"Header '$name': only '\\n' is allowed after '\\r' in value"))
        case LF =>
          if (c == '\t' || c == ' ') {
            foldDetected = true
            state = NonFold
          } else
            return ValidationFailure(
              new ValueValidationException(
                s"Header '$name': only ' ' and '\\t' are allowed after '\\n' in value"))
      }

      i += 1
    }

    if (state != NonFold) {
      ValidationFailure(
        new ValueValidationException(
          s"Header '$name': value must not end with '\\r' or '\\n'. Observed: " +
            (if (state == CR) "\\r" else "\\n")))
    } else if (foldDetected) {
      ObsFoldDetected
    } else {
      ValidationSuccess
    }
  }
}
