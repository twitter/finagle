package com.twitter.finagle.memcached.util

import com.google.common.base.Strings
import com.twitter.io.Buf
import scala.language.implicitConversions

private[finagle] object Bufs {

  final val INVALID_KEY_CHARACTERS = Set('\n'.toByte, '\u0000'.toByte, '\r'.toByte, ' '.toByte)

  /**
   * @return the Buf representation of non-empty and non-null Strings, else null
   */
  implicit def nonEmptyStringToBuf(str: String): Buf = {
    if (Strings.isNullOrEmpty(str))
      null
    else
      Buf.Utf8(str)
  }

  /**
   * @return the Buf representation of non-empty and non-null Strings
   * @note returns null if input is null
   */
  implicit def seqOfNonEmptyStringToBuf(strings: Traversable[String]): Seq[Buf] = {
    if (strings == null) {
      null
    }
    else {
      strings.map(nonEmptyStringToBuf).toSeq
    }
  }

  implicit class RichBuf(buffer: Buf) extends Seq[Byte] {

    /**
     * decode the Buf as a UTF8 string and split on delimiter
     * @param delimiter
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

    override def apply(idx: Int): Byte = _bytes.apply(idx)

    override def iterator: Iterator[Byte] = _bytes.iterator

    override def length: Int = buffer.length
  }
}
