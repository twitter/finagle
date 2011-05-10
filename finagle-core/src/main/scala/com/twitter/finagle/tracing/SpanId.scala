package com.twitter.finagle.tracing


import com.twitter.util.RichU64Long

final class SpanId(val self: Long) extends Proxy {
  def toLong = self
  override def toString: String = new RichU64Long(self).toU64HexString
}

object SpanId {
  def apply(spanId: Long) = new SpanId(spanId)
  def fromString(spanId: String): Option[SpanId] =
    try {
      Some(this(java.lang.Long.parseLong(spanId, 16)))
    } catch {
      case _ => None
    }
}
