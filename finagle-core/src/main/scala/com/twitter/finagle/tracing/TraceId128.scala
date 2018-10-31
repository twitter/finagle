package com.twitter.finagle.tracing

import scala.util.control.NonFatal

case class TraceId128(low: Option[SpanId], high: Option[SpanId])

object TraceId128 {
  val empty: TraceId128 = TraceId128(None, None)

  /**
   * Extracts the high 64bits (if set and valid) and low 64bits (if valid) from a B3 TraceID's string representation.
   *
   * @param spanId A 64bit or 128bit Trace ID.
   */
  def apply(spanId: String): TraceId128 = {
    try {
      val length = spanId.length
      val lower64Bits = if (length <= 16) spanId else spanId.substring(length - 16)

      val low =
        Some(SpanId(java.lang.Long.parseUnsignedLong(lower64Bits, 16)))

      val high =
        if (length == 32)
          Some(SpanId(java.lang.Long.parseUnsignedLong(spanId.substring(0, 16), 16)))
        else None

      TraceId128(low, high)
    } catch {
      case NonFatal(_) => empty
    }
  }
}
