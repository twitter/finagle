package com.twitter.finagle.http

import com.twitter.io.Buf
import java.nio.ByteBuffer

/**
 * Represents a piece of an HTTP stream, commonly referred to as a chunk.
 *
 * HTTP semantics treat trailing headers as the end of stream signal hence no writes (neither
 * trailers nor data) are allowed after them, only `close` (EOS) is valid. This is typically
 * enforced by an HTTP dispatcher in both clients and servers.
 *
 * Similarly, when consumed via [[com.twitter.io.Reader]], trailers are always followed by `None`,
 * readers's EOS. Users MUST read until the end of stream to ensure resource hygiene.
 *
 * The following example demonstrates one way of doing so: wiring in one extra read before
 * returning from a read-loop:
 *
 * {{{
 *   def accumulate(r: Reader[Chunk]): Future[(Buf, Option[HeaderMap])] = {
 *     def loop(acc: Buf, trailers: Option[HeaderMap]): Future[(Buf, Option[HeaderMap])] =
 *       r.read().flatMap {
 *         case Some(chunk) =>
 *           if (chunk.isLast && !chunk.trailers.isEmpty)
 *             loop(acc.concat(chunk.content), Some(chunk.trailers))
 *           else
 *             loop(acc.concat(chunk.content), None)
 *         case None =>
 *           Future.value(acc -> trailers)
 *       }
 *
 *     loop(Buf.Empty, None)
 *   }
 * }}}
 *
 * The HTTP dispatcher guarantees that `Chunk.Last` will be issued in the inbound stream
 * no matter if its `trailers` or `content` present.
 *
 * Note: when consuming interleaved HTTP streams (i.e., via `Reader.flatMap`) it's expected to
 * observe multiple trailers before reaching the EOS. These inter-stream trailers space out
 * individual HTTP streams from child readers.
 */
sealed abstract class Chunk {

  /**
   * The payload of this chunk. Can be empty if this is a last chunk.
   */
  def content: Buf

  /**
   * The trailing headers of this chunk. Can throw [[IllegalArgumentException]] if this is not the
   * last chunk.
   */
  def trailers: HeaderMap

  /**
   * Whether this chunk is last in the stream.
   */
  def isLast: Boolean
}

object Chunk {

  private final case class Cons(content: Buf) extends Chunk {
    def trailers: HeaderMap = throw new IllegalArgumentException("Not the last chunk")
    def isLast: Boolean = false
  }

  private final case class Last(content: Buf, trailers: HeaderMap) extends Chunk {
    def isLast: Boolean = true
  }

  /**
   * A last (end of stream) empty [[Chunk]] that has neither a payload nor trailing headers.
   */
  val lastEmpty: Chunk = last(HeaderMap.Empty)

  /**
   * Creates a last [[Chunk]] that carries trailing headers (`trailers`).
   */
  def last(trailers: HeaderMap): Chunk = last(Buf.Empty, trailers)

  /**
   * Creates a last [[Chunk]] that carries both payload (`content`) and trailing
   * headers (`trailers`).
   */
  def last(content: Buf, trailers: HeaderMap): Chunk = Last(content, trailers)

  /**
   * Creates a non last [[Chunk]] that carries a payload (`content`).
   */
  def fromBuf(content: Buf): Chunk = Cons(content)

  /**
   * A shortcut to create a [[Chunk]] out of a UTF-8 string.
   */
  def fromString(s: String): Chunk = fromBuf(Buf.Utf8(s))

  /**
   * A shortcut to create a [[Chunk]] out of a byte array.
   */
  def fromByteArray(ba: Array[Byte]): Chunk = fromBuf(Buf.ByteArray.Owned(ba))

  /**
   * A shortcut to create a [[Chunk]] out of a Java [[ByteBuffer]].
   */
  def fromByteBuffer(bb: ByteBuffer): Chunk = fromBuf(Buf.ByteBuffer.Owned(bb))

  /**
   * Creates a non last [[Chunk]] that carries a payload (`content`). This is an alias
   * for [[fromBuf]].
   */
  def apply(content: Buf): Chunk = fromBuf(content)
}
