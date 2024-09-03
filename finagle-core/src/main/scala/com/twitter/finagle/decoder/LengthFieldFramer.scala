package com.twitter.finagle.decoder

import com.twitter.io.Buf
import com.twitter.io.ByteReader
import scala.collection.mutable.ArrayBuffer

private[twitter] object LengthFieldFramer {

  class FrameTooLargeException(size: Long, max: Long)
      extends Exception(s"Frame of size $size exceeds max length of $max")

  private val ValidLengthFieldLengths = Set(1, 2, 3, 4, 8)
  private val NoFrames = IndexedSeq.empty[Buf]
}

/**
 * Decode a stream of bytes into protocol frames based on length fields found
 * in the frames. Frames are returned when all bytes are present. This decoder
 * is stateful and accumulates partial frames, so it is not threadsafe.
 *
 * The length adjustment field is used to account for situations where there are
 * a fixed number of additional bytes, such as a header, in the frame which do
 * not count towards the value in the length field.
 *
 * Example: The frame consists of a 1-byte magic header, a 2-byte field which is
 * the length of data, the data itself, and finally a 1-byte hash. The total
 * size of the frame is 1 + 2 + (length of data) + 1. Therefore, lengthAdjust is
 * set to 4 to account for the additional bytes.
 *
 * {{{
 * new LengthFieldDecoder(
 *   lengthFieldBegin = 1,   // The length field starts at index 1
 *   lengthFieldLength = 2,  // The length field is 2 bytes
 *   lengthAdjust = 4,       // 1 byte magic + 2 bytes length + 1 byte hash
 *   maxFrameLength = 1024,
 *   bigEndian = true)
 * }}}
 *
 * @param lengthFieldBegin The offset at which the length field begins in bytes.
 * @param lengthFieldLength The number of bytes in the length field. This must
 *                          be 1, 2, 3, 4, or 8.
 * @param lengthAdjust A length field based frame will typically contain
 *                     additional bytes which are not counted by the value in
 *                     the length field itself. Length adjust is used to account
 *                     for such bytes. The final size of a decoded frame is
                       `lengthAdjust` + the value read in the length field
 * @param maxFrameLength The largest frame to expect including the size of the
 *                       `lengthAdjust`
 * @param bigEndian Set to false if the length field is little endian. This has
 *                  no effect on the rest of the frame.
 */
private[twitter] class LengthFieldFramer(
  lengthFieldBegin: Int,
  lengthFieldLength: Int,
  lengthAdjust: Int,
  maxFrameLength: Int,
  bigEndian: Boolean)
    extends Framer {
  import LengthFieldFramer._

  private[this] var accum = Buf.Empty
  private[this] val lengthFieldEnd = lengthFieldBegin + lengthFieldLength

  require(
    ValidLengthFieldLengths.contains(lengthFieldLength),
    s"InvalidLengthFieldLength: $lengthFieldLength." +
      s"must be one of (${ValidLengthFieldLengths.mkString(", ")})"
  )

  require(lengthAdjust >= 0, s"Invalid lengthAdjust: $lengthAdjust. must be >= 0.")

  require(maxFrameLength > 0, s"Invalid maxFrameLength: $maxFrameLength. must be > 0.")

  require(
    lengthFieldBegin >= 0 && lengthFieldEnd <= maxFrameLength,
    "length field must be between 0 and maxFrameLength." +
      s"begin=$lengthFieldBegin, end=$lengthFieldEnd, max=$maxFrameLength"
  )

  require(
    lengthAdjust < maxFrameLength,
    s"Invalid lengthAdjust or maxFrameLength: $lengthAdjust , $maxFrameLength. " +
      "lengthAdjust must be < maxFrameLength"
  )

  /**
   * Read the next frame length out of `br` and advance `br` if the full frame
   * is present.
   * @return The length of the frame if it is complete, -1 otherwise.
   */
  private[this] def readNextFrameLength(br: ByteReader): Int = {
    if (br.remaining < lengthFieldEnd)
      return -1

    br.skip(lengthFieldBegin)

    val totalLength: Long = (lengthFieldLength match {
      case 1 => br.readUnsignedByte()
      case 2 => if (bigEndian) br.readUnsignedShortBE() else br.readUnsignedShortLE()
      case 3 => if (bigEndian) br.readUnsignedMediumBE() else br.readUnsignedMediumLE()
      case 4 => if (bigEndian) br.readUnsignedIntBE() else br.readUnsignedIntLE()
      case 8 => if (bigEndian) br.readLongBE() else br.readLongLE()
    }) + lengthAdjust

    if (totalLength > maxFrameLength) {
      throw new FrameTooLargeException(totalLength, maxFrameLength)
    }

    val remainingBytesInFrame = totalLength - lengthFieldEnd
    if (br.remaining < remainingBytesInFrame) {
      -1
    } else {
      br.skip(remainingBytesInFrame.toInt)
      totalLength.toInt
    }
  }

  /**
   * Consume the bytes in `buf`. If this results in one or more completed
   * frames, those frames are returned. Partial frames are stored and
   * returned when their remaining bytes arrive.
   *
   * @throws FrameTooLargeException if a frame is decoded to have a length
   *                                larger than `maxFrameLength`. This will
   *                                continue to throw the exception on each
   *                                subsequent call.
   * @param buf Additional bytes to consume
   * @return The frames decoded as a result of processing the additional bytes
   *         in `buf`. If no complete frames are available, an empty list is
   *         returned.
   */
  def apply(buf: Buf): IndexedSeq[Buf] = {
    // The accumulator is only modified upon receiving new bytes or after
    // successfully decoding one or more frames. This means that if any
    // exception is thrown during the decoding process, it will continue to be
    // thrown on EVERY subsequent call. Once an exception is encountered, the
    // channel should be closed and this decoder should be discarded.
    accum = accum.concat(buf)
    val br = ByteReader(accum)
    try {
      var frameLength = readNextFrameLength(br)

      if (frameLength > 0) {
        var frameCursor = 0
        val frames = new ArrayBuffer[Buf]

        while (frameLength >= 0) {
          frames += accum.slice(frameCursor, frameCursor + frameLength)
          frameCursor += frameLength
          frameLength = readNextFrameLength(br)
        }

        accum = accum.slice(frameCursor, accum.length)
        frames.asInstanceOf[IndexedSeq[Buf]]
      } else {
        NoFrames
      }
    } finally br.close()
  }
}
