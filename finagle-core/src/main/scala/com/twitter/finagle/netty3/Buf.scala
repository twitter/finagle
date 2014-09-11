package com.twitter.finagle.netty3

import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

/**
 * A [[com.twitter.io.Buf]] wrapper for
 * Netty [[org.jboss.netty.buffer.ChannelBuffer ChannelBuffers]].
 *
 * @note Since `ChannelBuffer`s are mutable, modifying the wrapped buffer
 * within `slice`s of a `ChannelBufferBuf` will modify the original wrapped
 * `ChannelBuffer`. Similarly, modifications to the original buffer will be
 * reflected in slices.
 *
 * @param buf The [[org.jboss.netty.buffer.ChannelBuffer]] to be wrapped in a
 * [[com.twitter.io.Buf]] interface.
 */
case class ChannelBufferBuf(buf: ChannelBuffer) extends Buf {
  def write(bytes: Array[Byte], off: Int): Unit = {
    val dup = buf.duplicate()
    dup.readBytes(bytes, off, dup.readableBytes)
  }

  def slice(i: Int, j: Int): Buf = {
    require(i >=0 && j >= 0, "Index out of bounds")

    if (j <= i || i >= length) Buf.Empty
    else if (i == 0 && j >= length) this
    else ChannelBufferBuf(buf.slice(i, (j-i) min (length-i)))
  }

  def length = buf.readableBytes
}
