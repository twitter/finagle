package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.mysql.util.BufferUtil

class LoginRequestSpec extends SpecificationWithJUnit {
  "Login Request" should {
    "encode" in {
      val username = "username"
      val password = "password"
      val salt = Array[Byte](70,38,43,66,74,48,79,126,76,66,
                              70,118,67,40,63,68,120,80,103,54)
      val req = LoginRequest(clientCap = Capability(0xfffff6ff),
                             username = username,
                             password = password,
                             serverCap = Capability(0xf7ff),
                             salt = salt,
                             database = Some("test"))

      val data = req.toByteArray
      val br = new BufferReader(data, Packet.headerSize)

      "capabilities" in {
        val mask = br.readInt
        mask mustEqual 0xfffff6ff
      }

      "maxPacket" in {
        br.skip(4)
        val max = br.readInt
        max mustEqual 0x10000000
      }

      "charset" in {
        br.skip(8)
        val charset = br.readByte
        charset mustEqual 33.toByte
      }

      "reserved bytes" in {
        val rbytes = data.drop(Packet.headerSize+9).take(23)
        rbytes must containAll(new Array[Byte](23))
      }

      "username" in {
        br.skip(32)
        br.readNullTerminatedString mustEqual username
      }

      "password" in {
        br.skip(32 + username.size+1)
        br.readLengthCodedString.getBytes must containAll(req.hashPassword)
      }
    }
  }
}