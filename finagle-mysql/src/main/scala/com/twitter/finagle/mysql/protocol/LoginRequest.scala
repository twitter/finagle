package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql.util.BufferUtil
import java.security.MessageDigest

case class LoginRequest(
  clientCapabilities: Capability = Capability(0xA68F),
  maxPacket: Int = 0x10000000,
  charset: Byte = 33.toByte, // TODO case class
  database: Option[String] = None, //name of schema to use initially
  username: String,
  password: String,
  sg: ServersGreeting
) extends Request(seq = 1.toByte) {
  val fixedBodySize = 34
  val dbNameSize = database map { _.size+1 } getOrElse(0)
  val dataSize = username.size + hashPassword.size + dbNameSize + fixedBodySize
  lazy val hashPassword = encryptPassword(password, sg.salt)

  override val data: Array[Byte] = {
    val bw = new BufferWriter(new Array[Byte](dataSize))
    bw.writeInt(clientCapabilities.mask)
    bw.writeInt(maxPacket)
    bw.writeByte(charset)
    bw.fill(23, 0.toByte) //23 reserved bytes - zeroed out 
    bw.writeNullTerminatedString(username)
    bw.writeLengthCodedString(new String(hashPassword))
    if(dbNameSize > 0 && sg.serverCapability.has(Capability.connectWithDB))
      bw.writeNullTerminatedString(database.get)
    bw.buffer
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
