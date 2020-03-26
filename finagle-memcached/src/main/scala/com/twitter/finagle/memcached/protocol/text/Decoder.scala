package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf

private[text] trait Decoder[Result >: Null] {

  /**
   * Decode a framed Buf into a Result. This method may emit 'null' to signal 'no message'.
   */
  def decode(buffer: Buf): Result
}

private[text] object Decoder {
  val TokenDelimiter: Byte = ' '

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
  )(
    continue: Seq[Buf] => Decoding
  ): Decoding = {
    val tokens = ParserUtils.splitOnWhitespace(frame)

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
