package com.twitter.finagle.postgres.values

import java.nio.charset.Charset
import java.security.MessageDigest

import io.netty.buffer.ByteBuf

import scala.annotation.tailrec

object Charsets {
  val Utf8 = Charset.forName("UTF-8")
}

object Buffers {
  /*
   * Reads a string with C-style '\0' terminator at the end from a buffer.
   */
  @throws(classOf[IndexOutOfBoundsException])
  def readCString(buffer: ByteBuf): String = {
    @tailrec
    def countChars(buf: ByteBuf, count: Int): Int = {
      if (!buffer.isReadable) {
        throw new IndexOutOfBoundsException("buffer ended, but '\\0' was not found")
      } else if (buffer.readByte() == 0) {
        count
      } else {
        countChars(buf, count + 1)
      }
    }

    buffer.markReaderIndex()
    // search for '\0'
    val count = countChars(buffer, 0)
    buffer.resetReaderIndex()

    // read a string without '\0'
    val result = buffer.toString(buffer.readerIndex(), count, Charsets.Utf8)
    // set reader index to the whole string length - including '\0'
    buffer.readerIndex(buffer.readerIndex() + count + 1)
    result
  }

  def readBytes(buffer: ByteBuf) = {
    val array = new Array[Byte](buffer.readableBytes())
    buffer.readBytes(array)
    array
  }

  def readString(buffer: ByteBuf, charset: Charset) = {
    val array = readBytes(buffer)
    new String(array, charset)
  }
}

object Md5Encryptor {
  /*
   * Encrypt a user/password combination using the MD5 algorithm.
   */
  @throws(classOf[IllegalArgumentException])
  def encrypt(user: Array[Byte], password: Array[Byte], salt: Array[Byte]): Array[Byte] = {

    require(user != null && user.length > 0, "user should not be empty")
    require(password != null && password.length > 0, "password should not be empty")
    require(salt != null && salt.length > 0, "salt should not be empty")

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

object Convert {
  def asShort(i : Int) = i.asInstanceOf[Short]
}

object Strings {
  val empty = new String
}