package com.twitter.finagle.tracing

import com.twitter.conversions.U64Ops._
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
  def toString(l: Long): String = l.toU64HexString

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
