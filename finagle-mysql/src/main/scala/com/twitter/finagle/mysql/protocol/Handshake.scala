package com.twitter.finagle.mysql.protocol

import java.security.MessageDigest

/**
 * Initial Result received from server during handshaking.
 */
case class ServersGreeting(
  protocol: Byte,
  version: String,
  threadId: Int,
  salt: Array[Byte], // 20 bytes from 2 different fields
  serverCap: Capability,
  language: Byte, // TODO case class
  status: Short // TODO case class
) extends Result

object ServersGreeting {
  def decode(packet: Packet): ServersGreeting = {
    val br = new BufferReader(packet.body)
    val protocol = br.readByte()
    val version = br.readNullTerminatedString()
    val threadId = br.readInt()
    val salt1 = br.take(8)
    br.skip(1) // 1 filler byte always 0x00
    val serverCap = Capability(br.readUnsignedShort())
    val language = br.readByte()
    val status = br.readShort()
    br.skip(13)
    val salt2 = br.take(12)
    
    ServersGreeting(
      protocol,
      version,
      threadId,
      Array.concat(salt1, salt2),
      serverCap,
      language,
      status
    )
  }
}

/**
 * Reply to ServerGreeting sent during handshaking phase.
 */
case class LoginRequest(
  username: String,
  password: String,
  database: Option[String] = None,
  serverCap: Capability,
  salt: Array[Byte],
  clientCap: Capability = Capability(0xA68F),
  maxPacket: Int = 0x10000000,
  charset: Byte = 33.toByte // TODO case class
) extends Request(seq = 1.toByte) {
  val fixedBodySize = 34
  val dbNameSize = database map { _.size+1 } getOrElse(0)
  val dataSize = username.size + hashPassword.size + dbNameSize + fixedBodySize
  lazy val hashPassword = encryptPassword(password, salt)

  override val data = {
    val bw = new BufferWriter(new Array[Byte](dataSize))
    val capability = if (dbNameSize == 0) clientCap - Capability.ConnectWithDB else clientCap
    bw.writeInt(capability.mask)
    bw.writeInt(maxPacket)
    bw.writeByte(charset)
    bw.fill(23, 0.toByte) // 23 reserved bytes - zeroed out 
    bw.writeNullTerminatedString(username)
    bw.writeLengthCodedString(hashPassword)
    if (dbNameSize > 0 && serverCap.has(Capability.ConnectWithDB))
      bw.writeNullTerminatedString(database.get)

    bw.toChannelBuffer
  }

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
