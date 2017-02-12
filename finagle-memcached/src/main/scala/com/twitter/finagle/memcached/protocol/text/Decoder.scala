package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf

private[text] trait Decoder {
  val TokenDelimiter: Byte = ' '

  // Constant for the length of a byte array that will contain a String representation of an Int,
  // which is used in the Decoder and Framer classes when converting a Buf to an Int
  private val MaxLengthOfIntString = Int.MinValue.toString.length

  // Array to re-use when converting a Buf to an Int
  val byteArrayForBuf2Int = ParserUtils.newByteArrayForBuf2Int()

  def decode(buffer: Buf): Decoding

  /**
   * Decodes a Buf Memcached line into a Decoding
   *
   * @param frame Buf to decode
   * @param needsData return the number of bytes needed, or `-1` if no more bytes
   *                  are necessary.
   * @param awaitData function to call when the decoded command needs more data
   * @param continue function that takes a sequence of Bufs and returns a Decoding.
   *                 Called when the decoded command does not need more data.
   *
   * @return a Decoding, or null if more data is needed
   */
  def decodeLine(
    frame: Buf,
    needsData: Seq[Buf] => Int,
    awaitData: (Seq[Buf], Int) => Unit
  )(continue: Seq[Buf] => Decoding): Decoding = {
    val bytes = Buf.ByteArray.Owned.extract(frame)
    val tokens = ParserUtils.split(bytes, TokenDelimiter)

    val bytesNeeded = if (tokens.nonEmpty) needsData(tokens) else -1
    if (bytesNeeded == -1) {
      continue(tokens)
    } else {
      awaitData(tokens, bytesNeeded)
      null
    }
  }

  def decodeData(bytesNeeded: Int, buffer: Buf)(continue: Buf => Decoding): Decoding = {
    if (buffer.length < bytesNeeded)
      null
    else
      continue(buffer)
  }
}
