package com.twitter.finagle.mysql.transport

import com.twitter.finagle.mysql.MysqlCharset
import org.scalatest.funsuite.AnyFunSuite

class BufferTest extends AnyFunSuite {
  val testBytes = Array[Byte](0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x78)

  test("read null terminated string") {
    val bytes = Array[Byte]('n', 'u', 'l', 'l', ' ', 't', 'e', 'r', 'm', 0x00)
    val br = MysqlBuf.reader(bytes)
    assert(br.readNullTerminatedString() == "null term")
  }

  test("read tiny length coded string") {
    val str = "test"
    val bytes = Array.concat(Array(str.size.toByte), str.getBytes)
    val br = MysqlBuf.reader(bytes)
    assert(br.readLengthCodedString(MysqlCharset.defaultCharset) == str)
  }

  def writerCtx() = new {
    val bytes = new Array[Byte](9)
    val bw = MysqlBuf.writer(bytes)
    val br = MysqlBuf.reader(bytes)
  }

  test("read/write tiny length coded int") {
    val ctx = writerCtx()
    import ctx._
    bw.writeVariableLong(250)
    assert(br.readVariableLong() == 250)
  }

  test("read/write short length coded int") {
    val ctx = writerCtx()
    import ctx._
    bw.writeVariableLong(65535)
    assert(br.readVariableLong() == 65535)
  }

  test("read/write medium length coded int") {
    val ctx = writerCtx()
    import ctx._
    bw.writeVariableLong(16777215)
    assert(br.readVariableLong() == 16777215)
  }

  test("read/write large length coded int") {
    val ctx = writerCtx()
    import ctx._
    bw.writeVariableLong(16777217L)
    assert(br.readVariableLong() == 16777217L)
  }

  test("read/write null terminated string") {
    val ctx = writerCtx()
    import ctx._
    val str = "test"
    bw.writeNullTerminatedString(str)
    assert(br.readNullTerminatedString() == str)

    val indexOfNull = str.length
    assert(bytes(indexOfNull) == 0x00)
  }

  test("read/write tiny length coded string") {
    val ctx = writerCtx()
    import ctx._
    val str = "test"
    bw.writeLengthCodedString(str, MysqlCharset.defaultCharset)
    assert(br.readLengthCodedString(MysqlCharset.defaultCharset) == str)
  }

  test("fail to write a negative value as a length-encoded integer") {
    val ctx = writerCtx()
    import ctx._
    intercept[IllegalStateException] { bw.writeVariableLong(-1) }
  }

  test("fail to read a negative value as a length-encoded integer") {
    val ctx = writerCtx()
    import ctx._
    bw.writeByte(254) // Signal that the next 8 bytes should be interpreted as a long
    bw.writeLongLE(-1L)

    intercept[IllegalStateException] { br.readVariableLong() }
  }

  test("read/write short length coded string") {
    val str = "test" * 100
    val len = MysqlBuf.sizeOfLen(str.size) + str.size
    val strAsBytes = new Array[Byte](len)
    val bw = MysqlBuf.writer(strAsBytes)
    bw.writeLengthCodedString(str, MysqlCharset.defaultCharset)

    val br = MysqlBuf.reader(strAsBytes)
    assert(br.readLengthCodedString(MysqlCharset.defaultCharset) == str)
  }

  test("read/write coded string with non-ascii characters") {
    val str = "バイトルドットコム"
    val strAsBytes = new Array[Byte](100)
    val bw = MysqlBuf.writer(strAsBytes)
    bw.writeLengthCodedString(str, MysqlCharset.defaultCharset)

    val br = MysqlBuf.reader(strAsBytes)
    assert(br.readLengthCodedString(MysqlCharset.defaultCharset) == str)
  }
}
