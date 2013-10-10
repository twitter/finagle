package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader, Packet}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OKTest extends FunSuite {
  val seq = 0x02.toByte
  val body = Array[Byte](0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00)
  val packet = Packet(seq, Buffer(body))

  test("Decode OK Packet") {
    val ok = OK(packet)
    assert(ok.isReturn)
    assert(ok().affectedRows === 0x00)
    assert(ok().insertId === 0x00)
    assert(ok().serverStatus === 0x02)
    assert(ok().warningCount === 0x00)
    assert(ok().message === "")
  }
}

@RunWith(classOf[JUnitRunner])
class ErrorTest extends FunSuite {
  val seq = 0x01.toByte
  val body = Array[Byte](0xff.toByte, 0x48, 0x04, 0x23, 0x48, 0x59, 0x30, 0x30,
    0x30, 0x4e, 0x6f, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x20,
    0x75, 0x73, 0x65, 0x64)
  val packet = Packet(seq, Buffer(body))

  test("Decode Error Packet") {
    val error = Error(packet)
    assert(error.isReturn)
    assert(error().code === 0x0448)
    assert(error().sqlState === "#HY000")
    assert(error().message === "No tables used")
  }
}

@RunWith(classOf[JUnitRunner])
class EofTest extends FunSuite {
  val seq = 0x01.toByte
  val body = Array[Byte](0xfe.toByte, 0x00, 0x00, 0x02, 0x00)
  val packet = Packet(seq, Buffer(body))

  test("Decode EOF Packet") {
    val eof = EOF(packet)
    assert(eof.isReturn)
    assert(eof().warnings === 0x00)
    assert(eof().serverStatus === 0x02)
  }
}

