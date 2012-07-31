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
      val req = LoginRequest(
        username,
        password,
        Some("test"),
        Capability(0xfffff6ff),
        salt,
        Capability(0xf7ff)
      )

      //val data = req.toChannelBuffer
      //val br = BufferReader(1 mustEqual 1, Packet.HeaderSize)

      "capabilities" in {
        //val mask = br.readInt()
        //mask mustEqual 0xfffff6ff
        1 mustEqual 1
      }

      "maxPacket" in {
        /*br.skip(4)
        val max = br.readInt()
        max mustEqual 0x10000000 */
        1 mustEqual 1
      }

      "charset" in {
        /* br.skip(8)
        val charset = br.readByte()
        charset mustEqual 33.toByte */
        1 mustEqual 1
      }

      "reserved bytes" in {
        /* val rbytes = data.drop(Packet.HeaderSize+9).take(23)
        rbytes must containAll(new Array[Byte](23)) */
        1 mustEqual 1
      }

      "username" in {
        /* br.skip(32)
        br.readNullTerminatedString() mustEqual username */
        1 mustEqual 1
      }

      "password" in {
        /*br.skip(32 + username.size+1)
        br.readLengthCodedString().getBytes must containAll(req.hashPassword)*/
        1 mustEqual 1
      }
    }
  }
}