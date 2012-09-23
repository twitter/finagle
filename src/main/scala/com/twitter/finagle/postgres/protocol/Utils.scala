package com.twitter.finagle.postgres.protocol

import org.jboss.netty.buffer.ChannelBuffer
import java.nio.charset.Charset
import scala.StringBuilder

object Charsets {

  val Utf8 = Charset.forName("UTF-8")

}

object Buffers {

  def writeCString(b: ChannelBuffer, content: String) {
    b.writeBytes(content.getBytes(Charsets.Utf8))
    b.writeByte(0)
  }

  def readCString(buffer: ChannelBuffer): String = {

    buffer.markReaderIndex()
    var count = 0
    var done = false
    while (!done) {
      done = buffer.readByte() == 0
      count += 1
    }
    buffer.resetReaderIndex()

    val result = buffer.toString(buffer.readerIndex(), count - 1, Charsets.Utf8)
    buffer.readerIndex(buffer.readerIndex() + count)
    result
  }

}

object HexDigits {
  private[this] val values = Array(
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'a', 'b', 'c', 'd', 'e', 'f')

  def apply(i: Int) = values(i)

}
