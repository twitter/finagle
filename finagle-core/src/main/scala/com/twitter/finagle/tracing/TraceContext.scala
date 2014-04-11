package com.twitter.finagle.tracing

import com.twitter.finagle.{Context, ContextHandler}
import com.twitter.finagle.util.ByteArrays
import com.twitter.io.Buf
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

private[finagle] object TraceContext {
  val Key = Buf.Utf8("com.twitter.finagle.tracing.TraceContext")
  val KeyBytes = Context.keyBytes(Key)
  val KeyBytesChannelBuffer = ChannelBuffers.wrappedBuffer(KeyBytes)

  /**
   * Serialize a TraceId into a tuple of key->value ChannelBuffers. Useful for
   * piecing together context pairs to give to the construct of `Tdispatch`.
   */
  private[finagle] def newKVTuple(traceId: TraceId): (ChannelBuffer, ChannelBuffer) =
    KeyBytesChannelBuffer.duplicate() -> ChannelBuffers.wrappedBuffer(TraceId.serialize(traceId))
}

/**
 * A context handler for Trace IDs.
 *
 * The wire format is (big-endian):
 *     ''spanId:8 parentId:8 traceId:8 flags:8''
 */
private[finagle] class TraceContext extends ContextHandler {
  val key = TraceContext.Key

  private[this] val local = new ThreadLocal[Array[Byte]] {
    override def initialValue() = new Array[Byte](32)
  }

  def handle(body: Buf) {
    if (body.length != 32)
      throw new IllegalArgumentException("Expected 32 bytes")

    val bytes = local.get()
    body.write(bytes, 0)

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

    Trace.setId(traceId)
  }

  def emit(): Option[Buf] =
    Some(Buf.ByteArray(TraceId.serialize(Trace.id)))
}
