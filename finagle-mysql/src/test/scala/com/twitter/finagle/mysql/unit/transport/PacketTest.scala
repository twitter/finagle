package com.twitter.finagle.exp.mysql.transport

import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PacketTest extends FunSuite {
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
