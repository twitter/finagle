package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.mysql.util.ByteArrayUtil

class LoginRequestSpec extends SpecificationWithJUnit {
  "Credentials Packet" should {
    "encode" in {
      val username = "username"
      val password = "password"
      val salt = Array[Byte](70,38,43,66,74,48,79,126,76,66,
                              70,118,67,40,63,68,120,80,103,54)
      val req = LoginRequest(Capability(0xfffff6ff),
                                0x10000000,
                                33.toByte,
                                None,
                                username,
                                password,
                                salt)
      val data = req.encoded

      "capabilities" in {
        val mask = ByteArrayUtil.read(data, Packet.headerSize, 4)
        mask mustEqual 0xfffff6ff
      }

      "maxPacket" in {
        val max = ByteArrayUtil.read(data, Packet.headerSize+4, 4)
        max mustEqual 0x10000000
      }

      "charset" in {
        val charset = ByteArrayUtil.read(data, Packet.headerSize+8, 1)
        charset mustEqual 33.toByte
      }

      "reserved bytes" in {
        val rbytes = data.drop(Packet.headerSize+9).take(23)
        rbytes must containAll(new Array[Byte](23))
      }

      "username" in {
        val uStr = Array.concat(username.getBytes, Array[Byte]('\0'.toByte))
        data.drop(Packet.headerSize+32).take(username.size+1) must containAll(uStr)
      }

      "password" in {
        val hash = req.hashPassword
        val pStr = Array.concat(Array[Byte](hash.length.toByte), hash)
        data.drop(Packet.headerSize+username.size+33) must containAll(pStr)
      }
    }
  }
}