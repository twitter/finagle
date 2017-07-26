package com.twitter.finagle.netty4

import com.twitter.io.Buf
import io.netty.buffer.ByteBuf

/**
 * An implementation of `ByteReader` that wraps a Netty 4 `ByteBuf`.
 *
 * @note This `ByteReader` implementation is not thread safe.
 *
 * @note This implementation does not retain an ownership interest in the passed `ByteBuf`,
 *       either directly or via generated values. It is up to the user to manage the
 *       resources of the underlying `ByteBuf`.
 */
private[finagle] final class CopyingByteBufByteReader(bb: ByteBuf)
    extends AbstractByteBufByteReader(bb) {

  /**
   * Returns a new buffer representing a slice of this buffer, delimited
   * by the indices `[cursor, remaining)`. Out of bounds indices are truncated.
   * Negative indices are not accepted.
   *
   * @note the returned `Buf` will be an explicit copy form the underlying `ByteBuf` to a
   *       `ByteArray` backed by only the bytes only usable the bytes.
   */
  def readBytes(n: Int): Buf = {
    if (n < 0) throw new IllegalArgumentException(s"'n' must be non-negative: $n")
    else if (n == 0) Buf.Empty
    else {
      val toRead = math.min(n, remaining)
      val arr = new Array[Byte](toRead)
      bb.readBytes(arr)
      Buf.ByteArray.Owned(arr)
    }
  }
}
