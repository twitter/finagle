package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql._
import java.security.MessageDigest

case class LoginRequest(
  clientCapabilities: Short = 0xA685.toShort,
  extendedClientCapabilities: Short = 0x8002.toShort,
  maxPacket: Int = 0x10000000,
  charset: Byte = 33.toByte, // TODO case class
  username: String, // null terminated string
  password: String,
  salt: Array[Byte]
) extends Packet {

  def size = 34 + username.size + hashPassword.size
  def number = 1
  def data = encode()

  def encode(): Array[Byte] = {
    val buffer = new Array[Byte](4 + size)
    var i = 0
    def write(n: Int, width: Int) = {
      Util.write(n, width, buffer, i)
      i += width
    }
    write(size, 3)
    write(number, 1)
    write(clientCapabilities, 2)
    write(extendedClientCapabilities, 2)
    write(maxPacket, 4)
    write(charset, 1)
    (i until i + 23) foreach { j => buffer(j) = 0.toByte ; i += 1 }
    Array.copy(username.getBytes, 0, buffer, i, username.size) ; i += username.size
    buffer(i) = 0.toByte ; i += 1
    write(hashPassword.size, 1)
    Array.copy(hashPassword, 0, buffer, i, hashPassword.size) ; i += hashPassword.size

    buffer
  }

  lazy val hashPassword = encryptPassword(password, salt)

  def encryptPassword(password: String, salt: Array[Byte]) = {
    val md = MessageDigest.getInstance("SHA-1");
    val hash1 = md.digest(password.getBytes)
    md.reset()
    val hash2 = md.digest(hash1)
    md.reset()
    md.update(salt)
    md.update(hash2)

    val digest = md.digest()
    (0 until digest.length) foreach { i =>
      digest(i) = (digest(i) ^ hash1(i)).toByte
    }
    digest
  }

}
