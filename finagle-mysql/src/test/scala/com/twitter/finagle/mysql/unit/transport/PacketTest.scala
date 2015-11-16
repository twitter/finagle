package com.twitter.finagle.exp.mysql.transport

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PacketTest extends FunSuite {
  val seq = 2.toShort
  val bytes = Array[Byte](0x01, 0x02, 0x03, 0x04)
  val body = Buffer(bytes)
  val packet = Packet(seq, body)

  test("Encode a Packet") {
    val buf = Buffer.fromChannelBuffer(packet.toChannelBuffer)
    val br = BufferReader(buf)
    assert(bytes.size == br.readInt24())
    assert(seq == br.readByte())
    assert(bytes === br.takeRest())
  }
}
