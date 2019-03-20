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
private[finagle] sealed abstract class Chunk {

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

private[finagle] object Chunk {

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
  val empty: Chunk = Last(Buf.Empty, HeaderMap.Empty)

  /**
   * Creates a non last [[Chunk]] that carries a payload (`content`) but no trailing headers.
   */
  def content(content: Buf): Chunk = Cons(content)

  /**
   * Creates a last [[Chunk]] that carries trailing headers (`trailers`).
   */
  def trailers(trailers: HeaderMap): Chunk = Last(Buf.Empty, trailers)

  /**
   * Creates a last [[Chunk]] that carries both payload (`content`) and trailing
   * headers (`trailers`).
   */
  def contentWithTrailers(content: Buf, trailers: HeaderMap): Chunk = Last(content, trailers)

  /**
   * A shortcut to create a [[Chunk]] out of a UTF-8 string.
   */
  def fromString(s: String): Chunk = Cons(Buf.Utf8(s))

  /**
   * A shortcut to create a [[Chunk]] out of a byte array.
   */
  def fromByteArray(ba: Array[Byte]): Chunk = Cons(Buf.ByteArray.Owned(ba))

  /**
   * A shortcut to create a [[Chunk]] out of a Java [[ByteBuffer]].
   */
  def fromByteBuffer(bb: ByteBuffer): Chunk = Cons(Buf.ByteBuffer.Owned(bb))
}
