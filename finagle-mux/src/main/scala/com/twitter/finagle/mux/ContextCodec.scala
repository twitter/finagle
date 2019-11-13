package com.twitter.finagle.mux

import com.twitter.io.{Buf, ByteReader, ByteWriter}
import scala.collection.mutable.ArrayBuffer

/**
 * Encoder and decoder for mux contexts
 *
 * Contexts are encoded as length delimited pairs, with each the key and
 * the value being prefixed with its length. For example, the context entries
 * ['foo': 'bar', 'biz': 'bazz'] would be encoded as:
 * 0x0003'foo'0x0003'bar'0x0003'biz'0x0004'bazz'
 *
 * In the syntax describing the mux protocol, this is represented as
 * (ckey~2 cval~2){nc}
 * where 'nc' is determined by either a prior knowledge, such an explicit
 * count such as in the Tdispatch and Rdispatch messages, or by consuming
 * the entire data source such as in the case of application headers.
 */
private[mux] object ContextCodec {

  /** The length of the encoded headers */
  def encodedLength(iter: Iterator[(Buf, Buf)]): Int = {
    var n = 0
    while (iter.hasNext) {
      iter.next() match {
        case (k, v) =>
          n += 2 + k.length + 2 + v.length
      }
    }
    n
  }

  /**
   * Decode the entire reader into context entries.
   *
   * @note this method does not take ownership of the `ByteReader` and it is up to the
   *       caller to manage it's lifetime.
   */
  def decodeAll(br: ByteReader): Seq[(Buf, Buf)] = {
    // We don't know the precise size and we don't try to guess.
    val acc = new ArrayBuffer[(Buf, Buf)]()
    decodeToBuffer(br, Int.MaxValue, acc)
    acc.toSeq
  }

  /**
   * Decode the reader into context entries, limiting the number of contexts decoded
   *
   * @note this method does not take ownership of the `ByteReader` and it is up to the
   *       caller to manage it's lifetime.
   */
  def decode(br: ByteReader, count: Int): Seq[(Buf, Buf)] = {
    val acc = new ArrayBuffer[(Buf, Buf)](count)
    decodeToBuffer(br, count, acc)
    acc.toSeq
  }

  private def decodeToBuffer(
    br: ByteReader,
    maxContexts: Int,
    contexts: ArrayBuffer[(Buf, Buf)]
  ): Unit =
    while (contexts.length < maxContexts && br.remaining > 0) {
      val k = br.readBytes(br.readShortBE())
      val v = br.readBytes(br.readShortBE())

      // For the context keys and values we want to decouple the backing array from the rest of
      // the Buf that composed the message. The body of the message is typically on the order of a
      // number of kilobytes and can be much larger, while the context entries are typically
      // on the order of < 100 bytes. Therefore, it is beneficial to decouple the lifetimes
      // at the cost of copying the context entries, but this ends up being a net win for the GC.
      // More context can be found in ``RB_ID=559066``.
      contexts += coerceTrimmed(k) -> coerceTrimmed(v)
    }

  /** Encode the headers into the `ByteWriter` */
  def encode(byteWriter: ByteWriter, iter: Iterator[(Buf, Buf)]): Unit = {
    while (iter.hasNext) {
      iter.next() match {
        case (k, v) =>
          byteWriter.writeShortBE(k.length)
          byteWriter.writeBytes(k)
          byteWriter.writeShortBE(v.length)
          byteWriter.writeBytes(v)
      }
    }
  }

  /**
   * Safely coerce the Buf to a representation that doesn't hold a reference to unused data.
   *
   * The resulting Buf will be either the Empty Buf if the input Buf has zero content, or a
   * ByteArray whose underlying array contains only the bytes exposed by the Buf, making a
   * copy of the data if necessary.
   *
   * For example, calling `coerceTrimmed` on a Buf that is a 10 byte slice of a larger Buf
   * which contains 1 KB of data will yield a new ByteArray backed by a new Array[Byte]
   * containing only the 10 bytes exposed by the passed slice.
   *
   * @note exposed for testing.
   */
  private[finagle] def coerceTrimmed(buf: Buf): Buf = buf match {
    case buf if buf.isEmpty => Buf.Empty
    case Buf.ByteArray.Owned(bytes, begin, end) if begin == 0 && end == bytes.length => buf
    case buf =>
      val bytes = Buf.ByteArray.Owned.extract(buf)
      Buf.ByteArray.Owned(bytes)
  }
}
