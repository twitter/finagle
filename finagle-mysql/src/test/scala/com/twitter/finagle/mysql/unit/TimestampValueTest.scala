package com.twitter.finagle.mysql.unit

import com.twitter.finagle.mysql._
import com.twitter.finagle.mysql.transport.MysqlBuf
import com.twitter.io.Buf
import java.nio.charset.StandardCharsets.{US_ASCII, UTF_8}
import java.sql.Timestamp
import java.util.TimeZone
import org.scalatest.funsuite.AnyFunSuite

class TimestampValueTest extends AnyFunSuite {
  val timestampValueLocal = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  test("encode timestamp") {
    val RawValue(_, _, true, bytes) =
      timestampValueLocal(Timestamp.valueOf("2014-10-09 08:27:53.123456789"))
    val br = MysqlBuf.reader(bytes)

    assert(br.readShortLE() == 2014)
    assert(br.readByte() == 10)
    assert(br.readByte() == 9)
    assert(br.readByte() == 8)
    assert(br.readByte() == 27)
    assert(br.readByte() == 53)
    assert(br.readIntLE() == 123456)
  }

  private def timestampBinary: Array[Byte] = {
    val bw = MysqlBuf.writer(new Array[Byte](11))
    bw.writeShortLE(2015)
      .writeByte(1)
      .writeByte(2)
      .writeByte(3)
      .writeByte(4)
      .writeByte(5)
      .writeIntLE(678901)

    Buf.ByteArray.Owned.extract(bw.owned())
  }

  test("decode binary timestamp with binary character set") {
    val timestampValueLocal(ts) =
      RawValue(Type.Timestamp, MysqlCharset.Binary, true, timestampBinary)
    assert(ts == Timestamp.valueOf("2015-01-02 03:04:05.678901"))
  }

  test("decode binary timestamp with utf8_general_ci character set") {
    val timestampValueLocal(ts) =
      RawValue(Type.Timestamp, MysqlCharset.Utf8_general_ci, true, timestampBinary)
    assert(ts == Timestamp.valueOf("2015-01-02 03:04:05.678901"))
  }

  test("decode text timestamp with binary character set") {
    val str = "2015-01-02 03:04:05.67890"

    val timestampValueLocal(ts) =
      RawValue(Type.Timestamp, MysqlCharset.Binary, false, str.getBytes(US_ASCII))
    assert(ts == Timestamp.valueOf("2015-01-02 03:04:05.6789"))
  }

  test("decode text timestamp with utf8_general_ci character set") {
    val str = "2015-01-02 03:04:05.67890"

    val timestampValueLocal(ts) =
      RawValue(Type.Timestamp, MysqlCharset.Utf8_general_ci, false, str.getBytes(UTF_8))
    assert(ts == Timestamp.valueOf("2015-01-02 03:04:05.6789"))
  }

  test("decode zero timestamp") {
    val str = "0000-00-00 00:00:00"

    val timestampValueLocal(ts) = RawValue(Type.Timestamp, MysqlCharset.Binary, false, str.getBytes)
    assert(ts == new Timestamp(0))
  }
}
