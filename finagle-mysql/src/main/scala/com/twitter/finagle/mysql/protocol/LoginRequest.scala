package com.twitter.finagle.mysql.protocol

/**
 * Client credentials request sent during the handshaking phase.
 */

import com.twitter.finagle.mysql.util.ByteArrayUtil
import java.security.MessageDigest

case class LoginRequest(
  clientCapabilities: Capability = Capability(0xA687),
  maxPacket: Int = 0x10000000,
  charset: Byte = 33.toByte, // TODO case class
  database: Option[String] = None, //initial database to use
  username: String,
  password: String,
  salt: Array[Byte]
) extends Request(seq = 1.toByte) {
  val fixedBodySize = 34
  val dbNameSize = database map { _.size+1 } getOrElse(0)
  val dataSize = username.size + hashPassword.size + dbNameSize + fixedBodySize
  lazy val hashPassword = encryptPassword(password, salt)

  override val data: Array[Byte] = {
    val buffer = new Array[Byte](dataSize)
    var offset = 0

    def write(n: Int, width: Int) = {
      ByteArrayUtil.write(n, buffer, offset, width)
      offset += width
    }

    write(clientCapabilities.mask, 4)
    write(maxPacket, 4)
    write(charset, 1)

    //23 reserved bytes - zeroed out
    (offset until offset + 23) foreach { j => buffer(j) = 0.toByte ; offset += 1 }

    //write username (including null terminating byte)
    ByteArrayUtil.writeNullTerminatedString(username, buffer, offset)
    offset += username.size + 1

    //write password size + hash
    ByteArrayUtil.writeLengthCodedString(new String(hashPassword), buffer, offset)
    offset += hashPassword.size + 1

    //write the initial db string if it exists
    if(dbNameSize > 0) {
      ByteArrayUtil.writeNullTerminatedString(database.get, buffer, offset)
      offset += dbNameSize + 1
    }

    buffer
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
