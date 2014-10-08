package com.twitter.finagle.tracing

import com.twitter.finagle.{ContextHelpers, ContextHandler}
import com.twitter.finagle.util.ByteArrays
import com.twitter.io.Buf
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

private[finagle] object TraceContext {
  val Key = Buf.Utf8("com.twitter.finagle.tracing.TraceContext")
  val KeyBytes = ContextHelpers.keyBytes(Key)
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
    
    val traceId = TraceId.deserialize(bytes).get()

    Trace.setId(traceId)
  }

  def emit(): Option[Buf] =
    Some(Buf.ByteArray(TraceId.serialize(Trace.id)))
}
