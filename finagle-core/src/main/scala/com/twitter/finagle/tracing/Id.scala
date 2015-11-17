package com.twitter.finagle.tracing

import com.twitter.finagle.util.ByteArrays
import com.twitter.util.RichU64String
import com.twitter.util.{Try, Return, Throw}
import com.twitter.util.NonFatal

/**
 * Defines trace identifiers.  Span IDs name a particular (unique)
 * span, while TraceIds contain a span ID as well as context (parentId
 * and traceId).
 */

final class SpanId(val self: Long) extends Proxy {
  def toLong = self

  override def toString: String = SpanId.toString(self)
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

  private def byteToChars(b: Byte): Array[Char] = lut(b+128)

  // This is invoked a lot, so they need to be fast.
  def toString(l: Long): String = {
    val b = new StringBuilder(16)
    b.appendAll(byteToChars((l>>56 & 0xff).toByte))
    b.appendAll(byteToChars((l>>48 & 0xff).toByte))
    b.appendAll(byteToChars((l>>40 & 0xff).toByte))
    b.appendAll(byteToChars((l>>32 & 0xff).toByte))
    b.appendAll(byteToChars((l>>24 & 0xff).toByte))
    b.appendAll(byteToChars((l>>16 & 0xff).toByte))
    b.appendAll(byteToChars((l>>8 & 0xff).toByte))
    b.appendAll(byteToChars((l & 0xff).toByte))
    b.toString
  }

  def apply(spanId: Long): SpanId = new SpanId(spanId)

  def fromString(spanId: String): Option[SpanId] =
    try {
      Some(SpanId(new RichU64String(spanId).toU64Long))
    } catch {
      case NonFatal(_) => None
    }
}

object TraceId {
  /**
   * Creates a TraceId with no flags set. See case class for more info.
   */
  def apply(
    traceId: Option[SpanId],
    parentId: Option[SpanId],
    spanId: SpanId,
    sampled: Option[Boolean]
  ): TraceId =
    TraceId(traceId, parentId, spanId, sampled, Flags())

  /**
   * Serialize a TraceId into an array of bytes.
   */
  def serialize(traceId: TraceId): Array[Byte] = {
    val flags = traceId._sampled match {
      case None =>
        traceId.flags
      case Some(true) =>
        traceId.flags.setFlag(Flags.SamplingKnown | Flags.Sampled)
      case Some(false) =>
        traceId.flags.setFlag(Flags.SamplingKnown)
    }

    val bytes = new Array[Byte](32)
    ByteArrays.put64be(bytes, 0, traceId.spanId.toLong)
    ByteArrays.put64be(bytes, 8, traceId.parentId.toLong)
    ByteArrays.put64be(bytes, 16, traceId.traceId.toLong)
    ByteArrays.put64be(bytes, 24, flags.toLong)
    bytes
  }

  /**
   * Deserialize a TraceId from an array of bytes.
   */
  def deserialize(bytes: Array[Byte]): Try[TraceId] = {
    if (bytes.length != 32) {
      Throw(new IllegalArgumentException("Expected 32 bytes"))
    } else {
      val span64 = ByteArrays.get64be(bytes, 0)
      val parent64 = ByteArrays.get64be(bytes, 8)
      val trace64 = ByteArrays.get64be(bytes, 16)
      val flags64 = ByteArrays.get64be(bytes, 24)

      val flags = Flags(flags64)
      val sampled = if (flags.isFlagSet(Flags.SamplingKnown)) {
        Some(flags.isFlagSet(Flags.Sampled))
      } else None

      val traceId = TraceId(
        if (trace64 == parent64) None else Some(SpanId(trace64)),
        if (parent64 == span64) None else Some(SpanId(parent64)),
        SpanId(span64),
        sampled,
        flags)
      Return(traceId)
    }
  }
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
  def traceId: SpanId = _traceId match {
    case None => parentId
    case Some(id) => id
  }

  def parentId: SpanId = _parentId match {
    case None => spanId
    case Some(id) => id
  }

  // debug flag overrides sampled to be true
  lazy val sampled = if (flags.isDebug) Some(true) else _sampled

  private[TraceId] def ids = (traceId, parentId, spanId)

  override def equals(other: Any) = other match {
    case other: TraceId => this.ids equals other.ids
    case _ => false
  }

  override def hashCode(): Int =
    ids.hashCode()

  override def toString = s"$traceId.$spanId<:$parentId"
}
