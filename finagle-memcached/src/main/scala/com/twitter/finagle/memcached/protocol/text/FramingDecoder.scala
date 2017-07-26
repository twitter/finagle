package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.Buf
import com.twitter.finagle.decoder.{Decoder => FinagleDecoder}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

private[finagle] final class FramingDecoder[Result](frameDecoder: FrameDecoder[Result])
    extends FinagleDecoder[Result] {

  private final val InitialBufferSize: Int = 4
  private final val MaxBufferSize: Int = 32

  private[this] var outputMessages: ArrayBuffer[Result] = new ArrayBuffer(InitialBufferSize)
  private[this] var accumulatedBytes: Buf = Buf.Empty

  def apply(data: Buf): IndexedSeq[Result] = {
    accumulatedBytes = accumulatedBytes.concat(data)
    decodeMessages()
    if (outputMessages.isEmpty) IndexedSeq.empty
    else if (outputMessages.length > MaxBufferSize) {
      // we don't want to leak memory by keeping a huge ArrayBuffer around, so just
      // send this one and make a new, smaller one
      val result = outputMessages
      outputMessages = new ArrayBuffer[Result](InitialBufferSize)
      result
    } else {
      // So save allocations we make a new ArrayBuffer with the exact right size and
      // copy the contents so we're not sending a half full collection or resizing
      val result = new ArrayBuffer[Result](outputMessages.length)
      result ++= outputMessages
      outputMessages.clear()
      result
    }
  }

  // We decode at least once, and continue of we continue to get more messages
  @tailrec
  private[this] def decodeMessages(): Unit = {
    val bytesNeeded = frameDecoder.nextFrameBytes()
    val continue =
      if (bytesNeeded == -1) decodeTextLine()
      else decodeRawBytes(bytesNeeded)

    if (continue) decodeMessages()
  }

  private[this] def decodeTextLine(): Boolean = {
    if (accumulatedBytes.isEmpty) false
    else {
      val frameLength = Framer.bytesBeforeLineEnd(accumulatedBytes)
      if (frameLength == -1) false // Not enough data for a tokens frame
      else {
        // We have received a text frame. Extract the frame and decode it.
        val frameBuf = accumulatedBytes.slice(0, frameLength)

        // Remove the extracted frame from the accumulator, stripping the newline (2 chars)
        accumulatedBytes = accumulatedBytes.slice(frameLength + 2, Int.MaxValue)
        frameDecoder.decodeData(frameBuf, outputMessages)
        true
      }
    }
  }

  private[this] def decodeRawBytes(nextByteFrameSize: Int): Boolean = {
    // We add the 2 bytes for the trailing '\r\n'
    if (accumulatedBytes.length < nextByteFrameSize + 2) false // Not enough data
    else {
      val dataFrame = accumulatedBytes.slice(0, nextByteFrameSize)
      accumulatedBytes = accumulatedBytes.slice(nextByteFrameSize + 2, Int.MaxValue)
      frameDecoder.decodeData(dataFrame, outputMessages)
      true
    }
  }
}
