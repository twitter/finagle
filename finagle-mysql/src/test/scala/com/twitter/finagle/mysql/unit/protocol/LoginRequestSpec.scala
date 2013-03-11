package com.twitter.finagle.exp.mysql.protocol

import org.specs.SpecificationWithJUnit

class LoginRequestSpec extends SpecificationWithJUnit {
  "Login Request" should {
    "encode" in {
      val username = "username"
      val password = "password"
      val salt = Array[Byte](70,38,43,66,74,48,79,126,76,66,
                              70,118,67,40,63,68,120,80,103,54)
      val req = LoginRequest(
        username,
        password,
        Some("test"),
        Capability(0xfffff6ff),
        salt,
        Capability(0xf7ff)
      )

      val br = BufferReader(req.toChannelBuffer)
      br.skip(4) // skip eader

      "capabilities" in {
        val mask = br.readInt()
        mask mustEqual 0xfffff6ff
      }

      "maxPacket" in {
        br.skip(4)
        val max = br.readInt()
        max mustEqual 0x10000000
      }

      "charset" in {
        br.skip(8)
        val charset = br.readByte()
        charset mustEqual 33.toByte
      }

      "reserved bytes" in {
        br.skip(9)
        val rbytes = br.take(23)
        rbytes must containAll(new Array[Byte](23))
      }

      "username" in {
        br.skip(32)
        br.readNullTerminatedString() mustEqual username
      }

      "password" in {
        br.skip(32 + username.size + 1)
        br.readLengthCodedBytes() must containAll(req.hashPassword)
      }
    }
  }
}
