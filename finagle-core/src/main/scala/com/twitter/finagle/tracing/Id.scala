package com.twitter.finagle.tracing

import com.twitter.util.{RichU64String, RichU64Long}

/**
 * Defines trace identifiers.  Span IDs name a particular (unique)
 * span, while TraceIds contain a span ID as well as context (parentId
 * and traceId).
 */

final class SpanId(val self: Long) extends Proxy {
  def toLong = self
  override def toString: String = new RichU64Long(self).toU64HexString
}

object SpanId {
  def apply(spanId: Long) = new SpanId(spanId)
  def fromString(spanId: String): Option[SpanId] =
    try {
      Some(this(new RichU64String(spanId).toU64Long))
    } catch {
      case _ => None
    }
}

final case class TraceId(
  _traceId: Option[SpanId],
  _parentId: Option[SpanId],
  spanId: SpanId,
  sampled: Option[Boolean]) // if true this trace is collected. if none, we have not decided.
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
