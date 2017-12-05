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

  /**
   * Convert s to a Boolean: True is "1", "t" or "true", false is all other values
   */
  def toBoolean(s: String): Boolean = {
    val v = s.toLowerCase
    v == "1" || v == "t" || v == "true"
  }

  /**
   * Split the provided text by a char separator into an array.
   *
   * @param s the String to parse
   * @param separatorChar the separator character
   * @return an IndexedSeq of parsed Strings
   */
  private[finagle] def split(s: String, separatorChar: Char): IndexedSeq[String] = {
    split(s, separatorChar, -1)
  }

  /**
   * Split the provided text by a char separator into an array with a maximum length
   *
   * @param s the String to parse
   * @param separatorChar the separator character
   * @param max the maximum number of elements in the result array
   * @return an IndexedSeq of parsed Strings
   */
  private[finagle] def split(s: String, separatorChar: Char, max: Int): IndexedSeq[String] = {
    if (s == null || s.length == 0) Vector.empty[String]
    else splitWorker(s, separatorChar, max)
  }

  private def splitWorker(s: String, separatorChar: Char, max: Int): IndexedSeq[String] = {
    // ignore the last element if it is a separator
    val length = if (s.charAt(s.length - 1) == separatorChar) s.length - 1 else s.length

    // count the number of elements
    var i = 0
    var separatorCount = 0
    while (i < length && (max == -1 || separatorCount <= max)) {
      if (s.charAt(i) == separatorChar) {
        separatorCount += 1
      }
      i += 1
    }

    val size = if (max == -1) separatorCount + 1 else math.min(separatorCount + 1, max)

    if (length == 0) Vector.empty[String]
    else {
      val values = new Array[String](size)

      var prev = 0
      var j = 0
      var count = 0
      while (j < length && count < size - 1) {
        if (s.charAt(j) == separatorChar) {
          if (prev <= j) {
            values(count) = s.substring(prev, j)
            count += 1
          }
          prev = j + 1
        }
        j += 1
      }
      values(size - 1) = s.substring(prev, length)
      values
    }
  }
}
