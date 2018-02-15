package com.twitter.finagle.tracing

import com.twitter.finagle.util.ByteArrays
import com.twitter.util.{Try, Return, Throw}
import java.lang.{Boolean => JBool}
import scala.util.control.NonFatal

/**
 * Defines trace identifiers.  Span IDs name a particular (unique)
 * span, while TraceIds contain a span ID as well as context (parentId
 * and traceId).
 */
final class SpanId(val self: Long) extends Proxy {
  def toLong: Long = self
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

  private def byteToChars(b: Byte): Array[Char] = lut(b + 128)

  // This is invoked a lot, so they need to be fast.
  def toString(l: Long): String = {
    val b = new StringBuilder(16)
    b.appendAll(byteToChars((l >> 56 & 0xff).toByte))
    b.appendAll(byteToChars((l >> 48 & 0xff).toByte))
    b.appendAll(byteToChars((l >> 40 & 0xff).toByte))
    b.appendAll(byteToChars((l >> 32 & 0xff).toByte))
    b.appendAll(byteToChars((l >> 24 & 0xff).toByte))
    b.appendAll(byteToChars((l >> 16 & 0xff).toByte))
    b.appendAll(byteToChars((l >> 8 & 0xff).toByte))
    b.appendAll(byteToChars((l & 0xff).toByte))
    b.toString
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
        if (length == 32) Some(SpanId(java.lang.Long.parseUnsignedLong(spanId.substring(0, 16), 16)))
        else None

      TraceId128(low, high)
    } catch {
      case NonFatal(_) => empty
    }
  }
}

object TraceId {

  /**
   * Creates a TraceId with no flags set and 64bit TraceID. See case class for more info.
   */
  def apply(
    traceId: Option[SpanId],
    parentId: Option[SpanId],
    spanId: SpanId,
    sampled: Option[Boolean]
  ): TraceId =
    TraceId(traceId, parentId, spanId, sampled, Flags(), None)

  /**
   * Creates a 64bit TraceID. See case class for more info.
   */
  def apply(
    traceId: Option[SpanId],
    parentId: Option[SpanId],
    spanId: SpanId,
    sampled: Option[Boolean],
    flags: Flags
  ): TraceId =
    TraceId(traceId, parentId, spanId, sampled, flags, None)

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

    // For backward compatibility for TraceID: 40 bytes if 128bit, 32 bytes if 64bit
    val bytes = new Array[Byte](if(traceId.traceIdHigh.isDefined) 40 else 32)
    ByteArrays.put64be(bytes, 0, traceId.spanId.toLong)
    ByteArrays.put64be(bytes, 8, traceId.parentId.toLong)
    ByteArrays.put64be(bytes, 16, traceId.traceId.toLong)
    ByteArrays.put64be(bytes, 24, flags.toLong)
    if (traceId.traceIdHigh.isDefined) ByteArrays.put64be(bytes, 32, traceId.traceIdHigh.get.toLong)
    bytes
  }

  /**
   * Deserialize a TraceId from an array of bytes.
   * Allows for 64-bit or 128-bit trace identifiers.
   */
  def deserialize(bytes: Array[Byte]): Try[TraceId] = {
    if (bytes.length != 32 && bytes.length != 40) {
      Throw(new IllegalArgumentException("Expected 32 or 40 bytes, was: " + bytes.length))
    } else {
      val span64 = ByteArrays.get64be(bytes, 0)
      val parent64 = ByteArrays.get64be(bytes, 8)
      val trace64 = ByteArrays.get64be(bytes, 16)
      val flags64 = ByteArrays.get64be(bytes, 24)

      val traceIdHigh = if (bytes.length == 40) Some(SpanId(ByteArrays.get64be(bytes, 32))) else None

      val flags = Flags(flags64)
      val sampled = if (flags.isFlagSet(Flags.SamplingKnown)) {
        Some(flags.isFlagSet(Flags.Sampled))
      } else None

      val traceId = TraceId(
        if (trace64 == parent64) None else Some(SpanId(trace64)),
        if (parent64 == span64) None else Some(SpanId(parent64)),
        SpanId(span64),
        sampled,
        flags,
        traceIdHigh
      )
      Return(traceId)
    }
  }
}

/**
 * A trace id represents one particular trace for one request.
 *
 * A request is composed of one or more spans, which are generally RPCs but
 * may be other in-process activity. The TraceId for each span is a tuple of
 * three ids:
 *
 *   1. a shared id common to all spans in an overall request (trace id)
 *   2. an id unique to this part of the request (span id)
 *   3. an id for the parent request that caused this span (parent id)
 *
 * For example, when service M calls service N, they may have respective
 * TraceIds like these:
 *
 * {{{
 *            TRACE ID         SPAN ID           PARENT ID
 * SERVICE M  e4bbb7c0f6a2ff07.a5f47e9fced314a2<:694eb2f05b8fd7d1
 *                   |                |
 *                   |                +-----------------+
 *                   |                                  |
 *                   v                                  v
 * SERVICE N  e4bbb7c0f6a2ff07.263edc9b65773b08<:a5f47e9fced314a2
 * }}}
 *
 * Parent id and trace id are optional when constructing a TraceId because
 * they are not present for the very first span in a request. In this case all
 * three ids in the resulting TraceId are the same:
 *
 * {{{
 *            TRACE ID         SPAN ID           PARENT ID
 * SERVICE A  34429b04b6bbf478.34429b04b6bbf478<:34429b04b6bbf478
 * }}}
 *
 * @param _traceId The low 64bits of the id for this request.
 * @param _parentId The id for the request one step up the service stack.
 * @param spanId The id for this particular request
 * @param _sampled Should we sample this request or not? True means sample, false means don't, none means we defer
 *                decision to someone further down in the stack.
 * @param flags Flags relevant to this request. Could be things like debug mode on/off. The sampled flag could eventually
 *              be moved in here.
 * @param traceIdHigh The high 64bits of the id for this request, when the id is 128bits.
 */
final case class TraceId(
  _traceId: Option[SpanId],
  _parentId: Option[SpanId],
  spanId: SpanId,
  _sampled: Option[Boolean],
  flags: Flags,
  traceIdHigh: Option[SpanId] = None
) {

  def this(
    _traceId: Option[SpanId],
    _parentId: Option[SpanId],
    spanId: SpanId,
    _sampled: Option[Boolean],
    flags: Flags
  ) = this(_traceId, _parentId, spanId, _sampled: Option[Boolean], flags: Flags, None)

  def traceId: SpanId = _traceId match {
    case None => parentId
    case Some(id) => id
  }

  def parentId: SpanId = _parentId match {
    case None => spanId
    case Some(id) => id
  }

  /**
   * Override [[_sampled]] to Some(true) if the debug flag is set.
   * @see [[getSampled]] for a Java-friendly API.
   */
  lazy val sampled: Option[Boolean] = if (flags.isDebug) Some(true) else _sampled

  /**
   * Java-friendly API to convert [[sampled]] to a [[Option]] of [[java.lang.Boolean]].
   * @since Java generics require objects, using [[sampled]] from
   *        Java would give an Option<Object> instead of Option<Boolean>
   */
  def getSampled(): Option[JBool] = sampled match {
    case Some(b) => Some(Boolean.box(b))
    case None => None
  }

  private[TraceId] def ids = (traceId, parentId, spanId, traceIdHigh)

  override def equals(other: Any): Boolean = other match {
    case other: TraceId => this.ids equals other.ids
    case _ => false
  }

  override def hashCode(): Int =
    ids.hashCode()

  override def toString = s"${if (traceIdHigh.isEmpty) "" else traceIdHigh.get}$traceId.$spanId<:$parentId"
}
