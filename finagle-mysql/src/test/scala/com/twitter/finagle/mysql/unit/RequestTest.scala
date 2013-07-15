package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.protocol.{BufferReader, Buffer, Command, Type}
import java.sql.{Timestamp, Date => SQLDate}
import java.util.Calendar
import java.util.Date
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RequestTest extends FunSuite with ShouldMatchers {

  test("encode simple request") {
    val seq = 3
    val bytes = Array[Byte](0x01, 0x02, 0x03, 0x04)
    val cb = Buffer.toChannelBuffer(bytes)

    val req = new Request(seq.toShort) {
      val data = cb
    }

    val br = BufferReader(req.toChannelBuffer)

    br.readInt24() should be === bytes.size
    br.readByte() should be === seq
    br.take(bytes.size) should be === bytes
  }

  test("encode simple command request") {
    val bytes = "table".getBytes
    val cmd = 0x00
    val req = new SimpleCommandRequest(cmd.toByte, bytes)

    val br = BufferReader(req.toChannelBuffer)
    br.readInt24() should be === bytes.size + 1 // cmd byte
    br.readByte() should be === 0x00
    br.readByte() should be === cmd
    br.take(bytes.size) should be === bytes
  }

  test("# of null bytes for all null values") {
    val numOfParams = 18
    val nullParams: Array[Any] = Array.fill(numOfParams)(null)
    val p = PreparedStatement(0, numOfParams)
    p.parameters = nullParams
    p.bindParameters() // set no new parameters
    val e = ExecuteRequest(p)
    val br = BufferReader(e.toChannelBuffer)
    br.skip(14) // packet header (4bytes) + payload header (10bytes)
    br.skip(1) // new params bound flag
    val restSize = br.takeRest().size
    restSize should be === ((numOfParams+7)/8)
  }

  // supported types
  val strVal = "test"
  val nonAsciiStrVal = "バイトルドットコム"
  val boolVal = true
  val byteVal = 1.toByte
  val shortVal = 2.toShort
  val intVal = 3
  val longVal = 4L
  val floatVal = 1.5F
  val doubleVal = 2.345
  val cal = Calendar.getInstance()
  val millis = cal.getTimeInMillis
  val timestamp = new Timestamp(millis)
  val sqlDate = new SQLDate(millis)
  val datetime = new Date(millis)
  val params = Array(
    strVal,
    nonAsciiStrVal,
    boolVal,
    byteVal,
    shortVal,
    null,
    intVal,
    longVal,
    floatVal,
    doubleVal,
    null,
    timestamp,
    sqlDate,
    datetime,
    null
  )
  val stmtId = 1
  val ps = PreparedStatement(stmtId, params.size)
  ps.parameters = params
  val flags, iteration = 0
  val req = ExecuteRequest(ps, flags.toByte, iteration)

  val br = BufferReader(req.toChannelBuffer)
  br.skip(4) // header

  val cmd = br.readByte()
  val id = br.readInt()
  val flg = br.readByte()
  val iter = br.readInt()

  test("statement Id, flags, and iteration count") {
    cmd should be === Command.COM_STMT_EXECUTE
    id should be === stmtId
    flg should be === flags
    iter should be === iteration
  }

  // null bit map
  val len = ((params.size + 7) / 8).toInt
  val bytes = br.take(len)
  val bytesAsBigEndian = bytes.reverse
  val bits = BigInt(bytesAsBigEndian)

  test("null bits") {
    for (i <- 0 until params.size) {
      if (params(i) == null)
        bits.testBit(i) should be === true
      else
        bits.testBit(i) should be === false
    }
  }

  val hasNewParams = br.readByte() == 1
  test("has new parameters") {
    hasNewParams should be === ps.hasNewParameters
  }

  if (hasNewParams) {
    test("type codes") {
      for (p <- params)
        br.readShort() should be === Type.getCode(p)
    }

    test("String") {
      br.readLengthCodedString() should be === strVal
    }

    test("Non-Ascii String") {
      br.readLengthCodedString() should be === nonAsciiStrVal
    }

    test("Boolean") {
      br.readByte() should be === (if (boolVal) 1 else 0)
    }

    test("Byte") {
      br.readByte() should be === byteVal
    }

    test("Short") {
      br.readShort() should be === shortVal
    }

    test("Int") {
      br.readInt() should be === intVal
    }

    test("Long") {
      br.readLong() should be === longVal
    }

    test("Float") {
      br.readFloat() should be === floatVal
    }

    test("Double") {
      br.readDouble() should be === doubleVal
    }

    test("java.sql.Timestamp") {
      val TimestampValue(ts) = TimestampValue(br.readLengthCodedBytes())
      ts  should be === timestamp
    }

    test("java.sql.Date") {
      val DateValue(d) = DateValue(br.readLengthCodedBytes())
      d.toString should be === sqlDate.toString
    }

    test("java.util.Date") {
      val TimestampValue(dt) = TimestampValue(br.readLengthCodedBytes())
      dt.getTime should be === datetime.getTime
    }
  }
}
