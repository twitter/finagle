package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.ByteReader
import scala.annotation.tailrec
import scala.collection.mutable

private[finagle] final class FramingDecoder[Result](frameDecoder: FrameDecoder[Result]) {

  def apply(reader: ByteReader, outputMessages: mutable.Buffer[Result]): Unit =
    decodeMessages(reader, outputMessages)

  // Recursively decode messages from `reader` and put into `outputMessages` until no more can be
  // decoded
  @tailrec
  private[this] def decodeMessages(
    reader: ByteReader,
    outputMessages: mutable.Buffer[Result]
  ): Unit = {
    val bytesNeeded = frameDecoder.nextFrameBytes()
    val continue =
      if (bytesNeeded == -1) decodeTextLine(reader, outputMessages)
      else decodeRawBytes(reader, bytesNeeded, outputMessages)

    if (continue) decodeMessages(reader, outputMessages)
  }

  private[this] def decodeTextLine(
    reader: ByteReader,
    outputMessages: mutable.Buffer[Result]
  ): Boolean = {
    if (reader.remaining == 0) false
    else {
      val frameLength = Framer.bytesBeforeLineEnd(reader)
      if (frameLength == -1) false // Not enough data for a tokens frame
      else {
        // We have received a text frame. Extract the frame and decode it.
        val frameBuf = reader.readBytes(frameLength)

        // Skip the newline (2 chars).
        reader.skip(2)

        frameDecoder.decodeData(frameBuf, outputMessages)
        true
      }
    }
  }

  private[this] def decodeRawBytes(
    reader: ByteReader,
    nextByteFrameSize: Int,
    outputMessages: mutable.Buffer[Result]
  ): Boolean = {
    // We add the 2 bytes for the trailing '\r\n'
    if (reader.remaining < nextByteFrameSize + 2) false // Not enough data
    else {
      val dataFrame = reader.readBytes(nextByteFrameSize)

      // skip the newline (2 chars)
      reader.skip(2)

      frameDecoder.decodeData(dataFrame, outputMessages)
      true
    }
  }
}
