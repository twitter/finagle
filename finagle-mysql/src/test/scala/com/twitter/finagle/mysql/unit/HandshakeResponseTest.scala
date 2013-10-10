package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.BufferReader
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HandshakeResponseTest extends FunSuite {
  val username = Some("username")
  val password = Some("password")
  val salt = Array[Byte](70,38,43,66,74,48,79,126,76,66,
                          70,118,67,40,63,68,120,80,103,54)
  val req = HandshakeResponse(
    username,
    password,
    Some("test"),
    Capability(0xfffff6ff),
    salt,
    Capability(0xf7ff),
    Charset.Utf8_general_ci,
    16777216
  )

  val br = BufferReader(req.toPacket.body)

  test("encode capabilities") {
    val mask = br.readInt()
    assert(mask === 0xfffff6ff)
  }

  test("maxPacketSize") {
    val max = br.readInt()
    assert(max === 16777216)
  }

  test("charset") {
    val charset = br.readByte()
    assert(charset === 33.toByte)
  }

  test("reserved bytes") {
    val rbytes = br.take(23)
    assert(rbytes.forall(_ == 0))
  }

  test("username") {
    assert(br.readNullTerminatedString() === username.get)
  }

  test("password") {
    assert(br.readLengthCodedBytes() === req.hashPassword)
  }
}