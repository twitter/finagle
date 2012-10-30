package com.twitter.finagle.postgres.protocol

import org.jboss.netty.buffer.ChannelBuffer
import java.nio.charset.Charset
import scala.StringBuilder
import java.security.MessageDigest

object Charsets {

  val Utf8 = Charset.forName("UTF-8")

}

object Buffers {

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

object Md5Encriptor {

  def encript(user: Array[Byte], password: Array[Byte], salt: Array[Byte]): Array[Byte] = {

    val inner = MessageDigest.getInstance("MD5")
    inner.update(password)
    inner.update(user)

    val outer = MessageDigest.getInstance("MD5")
    outer.update(Hex.valueOf(inner.digest).getBytes)
    outer.update(salt)

    ("md5" + Hex.valueOf(outer.digest)).getBytes
  }

}

object Hex {
  def valueOf(buf: Array[Byte]): String = buf.map("%02X" format _).mkString.toLowerCase
}
