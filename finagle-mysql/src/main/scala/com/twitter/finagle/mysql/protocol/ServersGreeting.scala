package com.twitter.finagle.mysql.protocol

/**
 * Server's greeting packet sent during the handshaking phase.
 */

import com.twitter.finagle.mysql.util.ByteArrayUtil

case class ServersGreeting(
  protocol: Byte,
  version: String,
  threadId: Int,
  salt: Array[Byte], // 20 bytes from 2 different fields
  serverCapability: Capability,
  language: Byte, // TODO case class
  status: Short // TODO case class
) extends Result

object ServersGreeting {

  def decode(packet: Packet): ServersGreeting = {
    var offset = 0

    def read(n: Int) = {
      val result = ByteArrayUtil.read(packet.body, offset, n)
      offset += n
      result
    }

    def readString() = {
      val result = ByteArrayUtil.readNullTerminatedString(packet.body, offset)
      offset += result.size + 1 //add 1 to account for '\0' character
      result
    }

    val protocol = read(1).toByte
    val version = readString()
    val threadId = read(4).toInt
    val salt1 = packet.body.drop(offset).take(8)
    offset += 9 // 8 + 1 null byte
    val serverCapabilities = Capability(read(2).toInt)
    val language = read(1).toByte
    val status = read(2).toShort
    offset += 13 // unused
    val salt2 = packet.body.drop(offset).take(12)
    offset += 13 // 12 + 1 null byte
    val salt = Array.concat(salt1, salt2)
    
    ServersGreeting(
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
