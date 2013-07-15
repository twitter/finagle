package com.twitter.finagle.exp.mysql.protocol

import com.twitter.finagle.exp.mysql.protocol.Capability._
import com.twitter.finagle.exp.mysql.{Result, Request}
import java.security.MessageDigest
import java.nio.charset.{Charset => JCharset}

/**
 * Initial Result received from server during handshaking.
 */
case class ServersGreeting(
  protocol: Byte,
  version: String,
  threadId: Int,
  salt: Array[Byte], // 20 bytes from 2 different fields
  serverCap: Capability,
  charset: Short,
  status: Short
) extends Result

object ServersGreeting {
  def decode(packet: Packet): ServersGreeting = {
    val br = BufferReader(packet.body)
    val protocol = br.readByte()
    val bytesVersion = br.readNullTerminatedBytes()
    val threadId = br.readInt()
    val salt1 = br.take(8)
    br.skip(1) // 1 filler byte always 0x00
    val serverCap = Capability(br.readUnsignedShort())
    val charset = br.readUnsignedByte()
    val version = new String(bytesVersion, Charset(charset))
    val status = br.readShort()
    br.skip(13)
    val salt2 = br.take(12)

    ServersGreeting(
      protocol,
      version,
      threadId,
      Array.concat(salt1, salt2),
      serverCap,
      charset,
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
  database: Option[String],
  clientCap: Capability,
  salt: Array[Byte],
  serverCap: Capability,
  charset: Short = Charset.Utf8_general_ci,
  maxPacket: Int = 0x10000000
) extends Request(seq = 1) {
  private[this] val fixedBodySize = 34
  private[this] val dbNameSize = database map { _.size+1 } getOrElse(0)
  private[this] val dataSize = username.size + hashPassword.size + dbNameSize + fixedBodySize
  lazy val hashPassword = encryptPassword(password, salt)

  override val data = {
    val bw = BufferWriter(new Array[Byte](dataSize))
    val newClientCap = if (dbNameSize == 0) clientCap - ConnectWithDB else clientCap
    bw.writeInt(newClientCap.mask)
    bw.writeInt(maxPacket)
    bw.writeByte(charset)
    bw.fill(23, 0.toByte) // 23 reserved bytes - zeroed out
    bw.writeNullTerminatedString(username, Charset(charset))
    bw.writeLengthCodedBytes(hashPassword)
    if (newClientCap.has(ConnectWithDB) && serverCap.has(ConnectWithDB))
      bw.writeNullTerminatedString(database.get, Charset(charset))

    bw.toChannelBuffer
  }

  private[this] def encryptPassword(password: String, salt: Array[Byte]) = {
    val md = MessageDigest.getInstance("SHA-1")
    val hash1 = md.digest(password.getBytes(Charset(charset).displayName))
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
