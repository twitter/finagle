package com.twitter.finagle.mysql.transport

import com.twitter.io.Buf
import org.scalatest.funsuite.AnyFunSuite

class PacketTest extends AnyFunSuite {
  val seq = 2.toShort
  val bytes = Array[Byte](0x01, 0x02, 0x03, 0x04)
  val body = Buf.ByteArray.Owned(bytes)
  val packet = Packet(seq, body)

  test("Encode a Packet") {
    val br = MysqlBuf.reader(packet.toBuf)
    assert(bytes.size == br.readMediumLE())
    assert(seq == br.readByte())
    assert(bytes === br.take(br.remaining))
  }
}
