package com.twitter.finagle.framer

import com.twitter.io.Buf

private[finagle] object FixedLengthFramer {
  val NoFrames: IndexedSeq[Buf] = IndexedSeq.empty[Buf]
}

/**
 * A stateful fixed-length framer.
 *
 * @param frameSize number of bytes in a frame.
 *
 * @note this class provides no thread-safety and must be explicitly synchronized.
 *       This implies a use-case where a single thread exclusively processes a
 *       byte stream.
 */
private[finagle] class FixedLengthFramer(frameSize: Int) extends Framer {
  import FixedLengthFramer._

  if (frameSize < 1)
    throw new IllegalArgumentException(s"frameSize must be greater than zero, saw: $frameSize")

  private[this] var accumulated: Buf = Buf.Empty

  /**
   * Decode the buffer `b` along with accumulated data into as
   * many fixed-size frames as possible.
   */
  def apply(b: Buf): IndexedSeq[Buf] = {
    val merged = accumulated.concat(b)
    val length = merged.length
    if (length < frameSize) {
      accumulated = merged
      NoFrames
    } else {
      // length >= frameSize
      val result = new Array[Buf](length / frameSize)
      var sliceIdx = 0
      var resultIdx = 0
      while (sliceIdx + frameSize <= length) {
        val slice = merged.slice(sliceIdx, sliceIdx + frameSize)
        result(resultIdx) = slice
        sliceIdx += frameSize
        resultIdx += 1
      }

      accumulated = merged.slice(sliceIdx, sliceIdx + frameSize)
      result
    }
  }
}
