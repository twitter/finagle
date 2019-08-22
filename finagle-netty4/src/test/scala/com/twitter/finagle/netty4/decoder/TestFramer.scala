package com.twitter.finagle.netty4.decoder

import com.twitter.finagle.decoder.Framer
import com.twitter.io.Buf

/**
 * A stateful fixed-length framer for testing purposes.
 *
 * @param frameSize number of bytes in a frame.
 */
private[finagle] class TestFramer(frameSize: Int) extends Framer {

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
      IndexedSeq.empty[Buf]
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
      result.toIndexedSeq
    }
  }
}
