package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.Buf
import scala.collection.mutable

private[memcached] abstract class FrameDecoder[Result] {

  /**
   * Return the number of raw bytes needed, or -1 if a text line is needed.
   * @note If a text line is needed, it is expected both a complete line and
   *       that the '\r\n' has been trimmed.
   */
  def nextFrameBytes(): Int

  /**
   * Decode a frame. The required format of the data is determined by a call to
   * `nextFrameBytes()`: if -1 is returned a text line (without the trailing '\r\n')
   * is expected. Otherwise, the next N bytes off the wire are expected and it is
   * expected that the trailing '\r\n' will be automatically dropped.
   */
  def decodeData(buf: Buf, results: mutable.Buffer[Result]): Unit
}
