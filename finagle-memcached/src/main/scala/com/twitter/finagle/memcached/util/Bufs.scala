package com.twitter.finagle.memcached.util

import com.twitter.io.Buf
import scala.language.implicitConversions

private[finagle] object Bufs {

  /**
   * @return the Buf representation of non-empty and non-null Strings, else null
   */
  implicit def nonEmptyStringToBuf(str: String): Buf = {
    if (str == null || str.isEmpty)
      null
    else
      Buf.Utf8(str)
  }

  /**
   * @return the Buf representation of non-empty and non-null Strings
   * @note returns null if input is null
   */
  implicit def seqOfNonEmptyStringToBuf(strings: Traversable[String]): Seq[Buf] =
    if (strings == null) {
      null
    } else {
      strings.toIterator.map(nonEmptyStringToBuf).toSeq
    }

  implicit class RichBuf(buffer: Buf) extends Seq[Byte] {

    /**
     * decode the Buf as a UTF8 string and split on delimiter
     * @return the UTF8 Buf encoded Strings resulting from the split
     */
    def split(delimiter: Char): Seq[Buf] = {
      val Buf.Utf8(decoded) = buffer
      decoded.split(delimiter).map { Buf.Utf8(_) }
    }

    /**
     * @return true if the Buf has prefix `prefix`, else false
     */
    def startsWith(prefix: Buf): Boolean =
      buffer.slice(0, prefix.length) == prefix

    /**
     * @return the decimal UTF-8 String (Long) decoding of the Buf
     */
    def toLong: Long = buffer match { case Buf.Utf8(s) => s.toLong }

    /**
     * @return the decimal UTF-8 String (Int) decoding of the Buf
     */
    def toInt: Int = buffer match { case Buf.Utf8(s) => s.toInt }

    /**
     * Seq[Byte] impl
     */
    private lazy val _bytes = {
      val bytes = new Array[Byte](buffer.length)
      buffer.write(bytes, 0)
      bytes
    }

    def apply(idx: Int): Byte = _bytes.apply(idx)

    def iterator: Iterator[Byte] = _bytes.iterator

    def length: Int = buffer.length
  }
}
