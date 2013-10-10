package com.twitter.finagle.exp.mysql

import java.sql.{Timestamp, Date => SQLDate}
import java.util.Calendar
import java.util.Date
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader}

@RunWith(classOf[JUnitRunner])
class ExecuteRequestTest extends FunSuite {
  test("# of null bytes for all null values") {
    val numOfParams = 18
    val nullParams: Array[Any] = Array.fill(numOfParams)(null)
    val ok = PrepareOK(0, 0, numOfParams, 0, Nil, Nil)
    val p = PreparedStatement(ok)
    p.parameters = nullParams
    p.bindParameters() // set no new parameters
    val e = ExecuteRequest(p)
    val br = BufferReader(e.toPacket.body)
    br.skip(10) // payload header (10bytes)
    br.skip(1) // new params bound flag
    val restSize = br.takeRest().size
    assert(restSize === ((numOfParams+7)/8))
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
  val ok = PrepareOK(stmtId, 0, params.size, 0, Nil, Nil)
  val ps = PreparedStatement(ok)
  ps.parameters = params
  val flags, iteration = 0
  val req = ExecuteRequest(ps, flags.toByte, iteration)

  val br = BufferReader(req.toPacket.body)

  val cmd = br.readByte()
  val id = br.readInt()
  val flg = br.readByte()
  val iter = br.readInt()

  // Note, this invariant can change if we decide to
  // make PreparedStatements persistent data structures.
  test("ExecuteRequest properly handles recycled PreparedStatements") {
    val execute1 = ExecuteRequest(ps)
    ps.parameters = (1 to params.size).map(_ => "new param").toArray
    val execute2 = ExecuteRequest(ps)
    val cb1 = execute1.toPacket.toChannelBuffer
    val cb2 = execute2.toPacket.toChannelBuffer
    assert(!cb1.equals(cb2))
  }

  test("statement Id, flags, and iteration count") {
    assert(cmd === Command.COM_STMT_EXECUTE)
    assert(id === stmtId)
    assert(flg === flags)
    assert(iter === iteration)
  }

  // null bit map
  val len = ((params.size + 7) / 8).toInt
  val bytes = br.take(len)
  val bytesAsBigEndian = bytes.reverse
  val bits = BigInt(bytesAsBigEndian)

  test("null bits") {
    for (i <- 0 until params.size) {
      if (params(i) == null)
        assert(bits.testBit(i) === true)
      else
        assert(bits.testBit(i) === false)
    }
  }

  val hasNewParams = br.readByte() == 1
  test("has new parameters") {
    assert(hasNewParams === ps.hasNewParameters)
  }

  if (hasNewParams) {
    test("type codes") {
      for (p <- params)
        assert(br.readShort() === Type.getCode(p))
    }

    test("String") {
      assert(br.readLengthCodedString() === strVal)
    }

    test("Non-Ascii String") {
      assert(br.readLengthCodedString() === nonAsciiStrVal)
    }

    test("Boolean") {
      assert(br.readByte() === (if (boolVal) 1 else 0))
    }

    test("Byte") {
      assert(br.readByte() === byteVal)
    }

    test("Short") {
      assert(br.readShort() === shortVal)
    }

    test("Int") {
      assert(br.readInt() === intVal)
    }

    test("Long") {
      assert(br.readLong() === longVal)
    }

    test("Float") {
      assert(br.readFloat() === floatVal)
    }

    test("Double") {
      assert(br.readDouble() === doubleVal)
    }

    test("java.sql.Timestamp") {
      val TimestampValue(ts) = TimestampValue(br.readLengthCodedBytes())
      assert(ts === timestamp)
    }

    test("java.sql.Date") {
      val DateValue(d) = DateValue(br.readLengthCodedBytes())
      assert(d.toString === sqlDate.toString)
    }

    test("java.util.Date") {
      val TimestampValue(dt) = TimestampValue(br.readLengthCodedBytes())
      assert(dt.getTime === datetime.getTime)
    }
  }
}
