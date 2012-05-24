package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql._

case class ServerGreetings(
  protocol: Byte,
  version: String,
  threadId: Int,
  salt: Array[Byte], // 20 bytes from 2 different fields
  serverCapabilities: Short, // TODO case class
  language: Byte, // TODO case class
  status: Short // TODO case class
) extends Result

object ServerGreetings {

  def decode(packet: Packet): ServerGreetings = {
    var i = 0
    def read(n: Int) = {
      val result = Util.read(packet.data, i, n)
      i += n
      result
    }
    def readString() = {
      val result = Util.readNullTerminatedString(packet.data, i)
      i += result.size + 1
      result
    }
    val protocol = read(1).toByte
    val version = readString()
    val threadId = read(4).toInt
    val salt1 = packet.data.drop(i).take(8)
    i += 9 // 8 + 1 null byte
    val serverCapabilities = read(2).toShort
    val language = read(1).toByte
    val status = read(2).toShort
    i += 13 // unused
    val salt2 = packet.data.drop(i).take(12)
    i += 13 // 12 + 1 null byte
    val salt = new Array[Byte](salt1.size + salt2.size)
    Array.copy(salt1, 0, salt, 0, 8)
    Array.copy(salt2, 0, salt, 8, 12)

    ServerGreetings(
      protocol,
      version,
      threadId,
      salt,
      serverCapabilities,
      language,
      status
    )
  }
}
