package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.decoder.{Framer => FinagleFramer}
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.{Buf, ByteReader}
import scala.collection.mutable.ArrayBuffer

private[memcached] object Framer {

  private sealed trait State
  private case object AwaitingTextFrame extends State
  private case class AwaitingDataFrame(bytesNeeded: Int) extends State

  private val EmptySeq = IndexedSeq.empty[Buf]

  /**
   * Return the number of bytes before `\r\n` (newline), or -1 if no newlines found
   */
  def bytesBeforeLineEnd(buf: Buf): Int = {
    val finder = new Buf.Processor {
      private[this] var prevCh: Byte = _
      def apply(byte: Byte): Boolean = {
        if (prevCh == '\r' && byte == '\n') false
        else {
          prevCh = byte
          true
        }
      }
    }
    val pos = buf.process(finder)
    if (pos == -1) -1 else pos - 1
  }

  /**
   * Return the number of bytes before `\r\n` (newline) in the reader's underlying
   * buffer, or -1 if no newlines found
   */
  def bytesBeforeLineEnd(reader: ByteReader): Int = {
    val finder = new Buf.Processor {
      private[this] var prevCh: Byte = _
      def apply(byte: Byte): Boolean = {
        if (prevCh == '\r' && byte == '\n') false
        else {
          prevCh = byte
          true
        }
      }
    }
    val pos = reader.process(finder)
    if (pos == -1) -1 else pos - 1
  }
}

/**
 * Frames Bufs into Memcached frames. Memcached frames are one of two types;
 * text frames and data frames. Text frames are delimited by `\r\n`. If a text
 * frame starts with the token `VALUE`, a data frame will follow. The length of the
 * data frame is given by the string representation of the third token in the
 * text frame. The data frame also ends with `\r\n`.
 *
 * For more information, see https://github.com/memcached/memcached/blob/master/doc/protocol.txt.
 *
 * To simplify the decoding logic, we have decoupled framing and decoding; however, because of the
 * complex framing logic, we must partially decode messages during framing to frame correctly.
 *
 * @note Class contains mutable state. Not thread-safe.
 */
private[memcached] trait Framer extends FinagleFramer {
  import Framer._

  private[this] var accum: Buf = Buf.Empty

  private[this] var state: State = AwaitingTextFrame

  /**
   * Using the current accumulation of Bufs, read the next frame. If no frame can be read,
   * return null.
   */
  private def extractFrame(): Buf =
    state match {
      case AwaitingTextFrame =>
        val frameLength = bytesBeforeLineEnd(accum)
        if (frameLength < 0) {
          null
        } else {

          // We have received a text frame. Extract the frame.
          val frameBuf: Buf = accum.slice(0, frameLength)

          // Remove the extracted frame from the accumulator, stripping the newline (2 chars)
          accum = accum.slice(frameLength + 2, accum.length)

          val tokens = ParserUtils.splitOnWhitespace(frameBuf)

          val bytesNeeded = dataLength(tokens)

          // If the frame starts with "VALUE", we expect a data frame to follow,
          // of length `bytesNeeded`.
          if (bytesNeeded != -1) state = AwaitingDataFrame(bytesNeeded)
          frameBuf
        }
      case AwaitingDataFrame(bytesNeeded) =>
        // A data frame ends with `\r\n', so we must wait for `bytesNeeded + 2` bytes.
        if (accum.length >= bytesNeeded + 2) {

          // Extract the data frame
          val frameBuf: Buf = accum.slice(0, bytesNeeded)

          // Remove the extracted frame from the accumulator, stripping the newline (2 chars)
          accum = accum.slice(bytesNeeded + 2, accum.length)
          state = AwaitingTextFrame
          frameBuf
        } else {
          null
        }
    }

  /**
   * Frame a Buf and any accumulated partial frames into as many Memcached frames as possible.
   */
  def apply(buf: Buf): IndexedSeq[Buf] = {
    accum = accum.concat(buf)
    var frame = extractFrame()

    if (frame != null) {
      // The average Gizmoduck memcached pipeline has 0-1 requests pending, and the average server
      // response is split into 2 memcached protocol frames, so we chose 2 as the initial array
      // size.
      val frames = new ArrayBuffer[Buf](2)
      do {
        frames += frame
        frame = extractFrame()
      } while (frame != null)

      frames.toIndexedSeq
    } else {
      EmptySeq
    }
  }

  /**
   * Given a sequence of Buf tokens that comprise a Memcached frame,
   * return the length of data expected in the next frame, or -1
   * if the length cannot be extracted.
   */
  def dataLength(tokens: Seq[Buf]): Int
}
