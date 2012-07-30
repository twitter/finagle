package com.twitter.finagle.tracing

import com.twitter.util.RichU64String

/**
 * Defines trace identifiers.  Span IDs name a particular (unique)
 * span, while TraceIds contain a span ID as well as context (parentId
 * and traceId).
 */

final class SpanId(val self: Long) extends Proxy {
  import SpanId.byteToStr

  def toLong = self

  // This is invoked a lot, so they need to be fast.
  override def toString: String = {
    val b = new StringBuilder(16)
    b.appendAll(byteToStr((self>>56 & 0xff).toByte))
    b.appendAll(byteToStr((self>>48 & 0xff).toByte))
    b.appendAll(byteToStr((self>>40 & 0xff).toByte))
    b.appendAll(byteToStr((self>>32 & 0xff).toByte))
    b.appendAll(byteToStr((self>>24 & 0xff).toByte))
    b.appendAll(byteToStr((self>>16 & 0xff).toByte))
    b.appendAll(byteToStr((self>>8 & 0xff).toByte))
    b.appendAll(byteToStr((self & 0xff).toByte))
    b.toString
  }
}

object SpanId {
  // StringBuilder.appendAll(char..) seems to be faster than
  // StringBuilder.append(string..)
  private val lut: Array[Array[Char]] = (
    for (b <- Byte.MinValue to Byte.MaxValue) yield {
      val bb = if (b < 0) b + 256 else b
      val s = "%02x".format(bb)
      Array(s(0), s(1))
    }
  ).toArray

  private def byteToStr(b: Byte) = lut(b+128)

  def apply(spanId: Long) = new SpanId(spanId)
  def fromString(spanId: String): Option[SpanId] =
    try {
      Some(this(new RichU64String(spanId).toU64Long))
    } catch {
      case _ => None
    }
}

object TraceId {
  /**
   * Creates a TraceId with no flags set. See case class for more info.
   */
  def apply(traceId: Option[SpanId],
            parentId: Option[SpanId],
            spanId: SpanId,
            sampled: Option[Boolean]): TraceId = TraceId(traceId, parentId, spanId, sampled, Flags())
}

/**
 * A trace id represents one particular trace for one request.
 * @param _traceId The id for this request.
 * @param _parentId The id for the request one step up the service stack.
 * @param spanId The id for this particular request
 * @param _sampled Should we sample this request or not? True means sample, false means don't, none means we defer
 *                decision to someone further down in the stack.
 * @param flags Flags relevant to this request. Could be things like debug mode on/off. The sampled flag could eventually
 *              be moved in here.
 */
final case class TraceId(
  _traceId: Option[SpanId],
  _parentId: Option[SpanId],
  spanId: SpanId,
  _sampled: Option[Boolean],
  flags: Flags)
{
  def traceId = _traceId getOrElse parentId
  def parentId = _parentId getOrElse spanId
  // debug flag overrides sampled to be true
  lazy val sampled = if (flags.isDebug) Some(true) else _sampled

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
