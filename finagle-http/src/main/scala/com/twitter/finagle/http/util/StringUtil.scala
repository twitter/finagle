package com.twitter.finagle.http.util


object StringUtil {

  private val SomeIntRegex = """\A\s*(-?\d+).*\Z""".r

  /**
   * Convert s to a Int liberally: initial whitespace and zeros are
   * skipped, non-digits after the number are ignored, and the default is 0.
   */
  def toSomeShort(s: String): Short = {
    SomeIntRegex.findFirstMatchIn(s) match {
      case Some(sMatch) =>
        try {
          sMatch.group(1).toShort
        } catch {
          case e: NumberFormatException => 0
        }
      case None =>
        0
    }
  }

  /**
   * Convert s to an Int liberally: initial whitespace and zeros are
   * skipped, non-digits after the number are ignored, and the default is 0.
   */
  def toSomeInt(s: String): Int = {
    SomeIntRegex.findFirstMatchIn(s) match {
      case Some(sMatch) =>
        try {
          sMatch.group(1).toInt
        } catch {
          case e: NumberFormatException => 0
        }
      case None =>
        0
    }
  }

  /**
   * Convert s to a Long liberally: initial whitespace and zeros are
   * skipped, non-digits after the number are ignored, and the default is 0L.
   */
  def toSomeLong(s: String): Long = {
    SomeIntRegex.findFirstMatchIn(s) match {
      case Some(sMatch) =>
        try {
          sMatch.group(1).toLong
        } catch {
          case e: NumberFormatException => 0L
        }
      case None =>
        0L
    }
  }
}
