package com.twitter.finagle.exp.mysql.transport

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BufferTest extends FunSuite {
  val bytes = Array[Byte](0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x78)

  test("read Byte") {
    val br = BufferReader(bytes)
    assert(br.readByte() == 0x11)
    assert(br.readByte() == 0x22)
    assert(br.readByte() == 0x33)
    assert(br.readByte() == 0x44)
    assert(br.readByte() == 0x55)
    assert(br.readByte() == 0x66)
    assert(br.readByte() == 0x77)
    assert(br.readByte() == 0x78)
  }

  test("read Short") {
    val br = BufferReader(bytes)
    assert(br.readShort() == 0x2211)
    assert(br.readShort() == 0x4433)
    assert(br.readShort() == 0x6655)
    assert(br.readShort() == 0x7877)
  }

  test("read Int24") {
    val br = BufferReader(bytes)
    assert(br.readInt24() == 0x332211)
    assert(br.readInt24() == 0x665544)
    assert(br.readShort() == 0x7877)
  }

  test("read Int") {
    val br = BufferReader(bytes)
    br.readInt() == 0x44332211
    br.readInt() == 0x78776655
  }

  test("read Signed Int") {
    val n = 0xfffff6ff
    val br = BufferReader(Array[Byte](0xff.toByte, 0xf6.toByte, 0xff.toByte, 0xff.toByte))
    assert(n == br.readInt())
  }

  test("read Long") {
    val br = BufferReader(bytes)
    assert(br.readLong() == 0x7877665544332211L)
  }

  test("read null terminated string") {
    val str = "Null Terminated String\0"
    val br = BufferReader(str.getBytes)
    assert(str.take(str.size-1) == br.readNullTerminatedString())
  }

  test("read tiny length coded string") {
    val str = "test"
    val bytes = Array.concat(Array(str.size.toByte), str.getBytes)
    val br = BufferReader(bytes)
    assert(str == br.readLengthCodedString())
  }

  def writerCtx() = new {
    val bytes = new Array[Byte](9)
    val bw = BufferWriter(bytes)
    val br = BufferReader(bytes)
  }

  test("write byte") {
    val ctx = writerCtx()
    import ctx._
    bw.writeByte(0x01.toByte)
    assert(0x01 == br.readByte())
  }

  test("write Short") {
    val ctx = writerCtx()
    import ctx._
    bw.writeShort(0xFE.toShort)
    assert(0xFE == br.readShort())
  }

  test("write Int24") {
    val ctx = writerCtx()
    import ctx._
    bw.writeInt24(0x872312)
    assert(0x872312 == br.readUnsignedInt24())
  }

  test("write Int") {
    val ctx = writerCtx()
    import ctx._
    bw.writeInt(0x98765432)
    assert(0x98765432 == br.readInt())
  }

  test("write Long") {
    val ctx = writerCtx()
    import ctx._
    bw.writeLong(0x7877665544332211L)
    assert(0x7877665544332211L == br.readLong)
  }

  test("tiny length coded binary") {
    val ctx = writerCtx()
    import ctx._
    bw.writeLengthCodedBinary(250)
    assert(br.readLengthCodedBinary() == 250)
  }

  test("short length coded binary") {
    val ctx = writerCtx()
    import ctx._
    bw.writeLengthCodedBinary(65535)
    assert(br.readLengthCodedBinary() == 65535)
  }

  test("medium length coded binary") {
    val ctx = writerCtx()
    import ctx._
    bw.writeLengthCodedBinary(16777215)
    assert(br.readLengthCodedBinary() == 16777215)
  }

  test("large length coded binary") {
    val ctx = writerCtx()
    import ctx._
    bw.writeLengthCodedBinary(16777217L)
    assert(br.readLengthCodedBinary() == 16777217L)
  }

  test("null terminated string") {
    val ctx = writerCtx()
    import ctx._
    val str = "test\0"
    bw.writeNullTerminatedString(str)
    assert(str.take(str.length-1) == br.readNullTerminatedString())
  }

  test("tiny length coded string") {
    val ctx = writerCtx()
    import ctx._
    val str = "test"
    bw.writeLengthCodedString(str)
    assert(str == br.readLengthCodedString())
  }

  test("short length coded string") {
    val str = "test" * 100
    val len = Buffer.sizeOfLen(str.size) + str.size
    val strAsBytes = new Array[Byte](len)
    val bw = BufferWriter(strAsBytes)
    bw.writeLengthCodedString(str)

    val br = BufferReader(strAsBytes)
    assert(str == br.readLengthCodedString())
  }

  test("coded string with non-ascii characters") {
    val str = "バイトルドットコム"
    val strAsBytes = new Array[Byte](100)
    val bw = BufferWriter(strAsBytes)
    bw.writeLengthCodedString(str)

    val br = BufferReader(strAsBytes)
    assert(str == br.readLengthCodedString())
  }
}
