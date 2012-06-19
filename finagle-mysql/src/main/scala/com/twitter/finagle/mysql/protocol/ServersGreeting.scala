package com.twitter.finagle.mysql.protocol

/**
 * Server's greeting result received during the handshaking phase.
 */

import com.twitter.finagle.mysql.util.BufferUtil

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
    val br = new BufferReader(packet.body)
    val protocol = br.readByte
    val version = br.readNullTerminatedString
    val threadId = br.readInt
    val salt1 = br.take(8)
    br.skip(9)
    val serverCapabilities = Capability(br.read(2).toInt)
    val language = br.readByte
    val status = br.readShort
    br.skip(13)
    val salt2 = br.take(12)
    
    ServersGreeting(
      protocol,
      version,
      threadId,
      Array.concat(salt1, salt2),
      serverCapabilities,
      language,
      status
    )
  }
}
