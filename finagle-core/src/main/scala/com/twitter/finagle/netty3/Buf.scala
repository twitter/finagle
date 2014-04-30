package com.twitter.finagle.netty3

import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

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
