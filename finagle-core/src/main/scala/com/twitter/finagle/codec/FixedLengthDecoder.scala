package com.twitter.finagle.codec

import com.twitter.io.Buf
import scala.reflect.ClassTag

/**
 * A stateful fixed-length frame decoder.
 *
 * @param frameSize number of bytes in a `T` frame.
 * @param decodeFrame function which decodes a [[com.twitter.io.Buf]] of
 *                    `frameSize` bytes into a `T` value.
 * @tparam T the frame type.
 *
 * @note this class provides no thread-safety and must be explicitly synchronized.
 *       This implies a use-case where a single thread exclusively processes a
 *       byte stream.
 */
private[finagle] class FixedLengthDecoder[T: ClassTag](frameSize: Int, decodeFrame: Buf => T)
  extends FrameDecoder[T] {

  if (frameSize < 1)
    throw new IllegalArgumentException(s"frameSize must be greater than zero, saw: $frameSize")

  private[this] var accumulated: Buf = Buf.Empty
  private[this] val NoFrames: IndexedSeq[T] = IndexedSeq.empty[T]

  /**
   * Decode the buffer `b` along with accumulated data into as
   * many `T`-typed frames as possible.
   */
  def apply(b: Buf): IndexedSeq[T] = {
    val merged = accumulated.concat(b)
    val length = merged.length
    if (length < frameSize) {
      accumulated = merged
      NoFrames
    } else {
      // length >= frameSize
      val result = new Array[T](length / frameSize)
      var sliceIdx = 0
      var resultIdx = 0
      while (sliceIdx + frameSize <= length) {
        val slice = merged.slice(sliceIdx, sliceIdx + frameSize)
        result(resultIdx) = decodeFrame(slice)
        sliceIdx += frameSize
        resultIdx += 1
      }

      accumulated = merged.slice(sliceIdx, sliceIdx + frameSize)
      result
    }
  }
}
