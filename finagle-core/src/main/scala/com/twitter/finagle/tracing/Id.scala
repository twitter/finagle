package com.twitter.finagle.tracing

/**
 * Defines trace identifiers.  Span IDs name a particular (unique)
 * span, while TraceIds contain a span ID as well as context (parentId
 * and traceId).
 */

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

final case class TraceId(
  _traceId: Option[SpanId],
  _parentId: Option[SpanId],
  spanId: SpanId,
  sampled: Boolean) // if true this trace is not collected
{
  def traceId = _traceId getOrElse parentId
  def parentId = _parentId getOrElse spanId

  override def equals(other: Any) = other match {
    case other: TraceId =>
      (this.traceId equals other.traceId) &&
      (this.parentId equals other.parentId) &&
      (this.spanId equals other.spanId)
    case _ => false
  }


  override def toString =
    "%s.%s<:%s".format(traceId, spanId, parentId)
}
