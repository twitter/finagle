package com.twitter.finagle.tracing

import java.io.Serializable
import scala.util.control.NonFatal

/**
 * Defines trace identifiers.  Span IDs name a particular (unique)
 * span, while TraceIds contain a span ID as well as context (parentId
 * and traceId).
 */
final class SpanId(val self: Long) extends Proxy with Serializable {
  def toLong: Long = self
  override def toString: String = SpanId.toString(self)
}

object SpanId {
  private[this] val lut: Array[Array[Char]] = (
    for (b <- Byte.MinValue to Byte.MaxValue) yield {
      val bb = if (b < 0) b + 256 else b
      val s = "%02x".format(bb)
      Array(s(0), s(1))
    }
  ).toArray

  private def byteToChars(b: Byte): Array[Char] = lut(b + 128)

  private[this] val chars = new ThreadLocal[Array[Char]] {
    override def initialValue(): Array[Char] = new Array[Char](16)
  }

  private[this] def append(chs: Array[Char], offset: Int, input: Array[Char]): Unit = {
    chs(offset) = input(0)
    chs(offset + 1) = input(1)
  }

  def toString(l: Long): String = {
    val chs = chars.get()
    append(chs, 0, byteToChars((l >> 56 & 0xff).toByte))
    append(chs, 2, byteToChars((l >> 48 & 0xff).toByte))
    append(chs, 4, byteToChars((l >> 40 & 0xff).toByte))
    append(chs, 6, byteToChars((l >> 32 & 0xff).toByte))
    append(chs, 8, byteToChars((l >> 24 & 0xff).toByte))
    append(chs, 10, byteToChars((l >> 16 & 0xff).toByte))
    append(chs, 12, byteToChars((l >> 8 & 0xff).toByte))
    append(chs, 14, byteToChars((l & 0xff).toByte))
    new String(chs)
  }

  def apply(spanId: Long): SpanId = new SpanId(spanId)

  def fromString(spanId: String): Option[SpanId] =
    try {
      // Tolerates 128 bit X-B3-TraceId by reading the right-most 16 hex
      // characters (as opposed to overflowing a U64 and starting a new trace).
      // For TraceId, prefer TraceId128#apply.
      val length = spanId.length
      val lower64Bits = if (length <= 16) spanId else spanId.substring(length - 16)
      Some(SpanId(java.lang.Long.parseUnsignedLong(lower64Bits, 16)))
    } catch {
      case NonFatal(_) => None
    }
}
