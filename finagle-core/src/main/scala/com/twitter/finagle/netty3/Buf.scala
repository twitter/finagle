package com.twitter.finagle.netty3

import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

case class ChannelBufferBuf(buf: ChannelBuffer) extends Buf {
  def write(bytes: Array[Byte], off: Int): Unit = {
    val dup = buf.duplicate()
    dup.readBytes(bytes, off, dup.readableBytes)
  }
  
  def slice(i: Int, j: Int): Buf = copy(buf.slice(i, j-i))
  def length = buf.readableBytes
}
