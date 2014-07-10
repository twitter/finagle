package com.twitter.finagle.exp.mysql

import java.sql.{Timestamp, Date => SQLDate}
import java.util.Calendar
import java.util.Date
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader}

@RunWith(classOf[JUnitRunner])
class SimpleCommandRequestTest extends FunSuite {
  test("encode") {
    val bytes = "table".getBytes
    val cmd = 0x00
    val req = new SimpleCommandRequest(cmd.toByte, bytes)
    val buf = Buffer.fromChannelBuffer(req.toPacket.toChannelBuffer)
    val br = BufferReader(buf)
    assert(br.readInt24() === bytes.size + 1) // cmd byte
    assert(br.readByte() === 0x00)
    assert(br.readByte() === cmd)
    assert(br.take(bytes.size) === bytes)
  }
}

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

@RunWith(classOf[JUnitRunner])
class ExecuteRequestTest extends FunSuite {
  test("null values") {
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
  // create a prepared statement
  val stmtId = 1
  val ok = PrepareOK(1, 0, params.size, 0, Nil, Nil)
  val ps = PreparedStatement(ok)
  ps.parameters = params
  val flags, iteration = 0
  val req = ExecuteRequest(ps, flags.toByte, iteration)
  val br = BufferReader(req.toPacket.body)

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

  val cmd = br.readByte()
  val id = br.readInt()
  val flg = br.readByte()
  val iter = br.readInt()
  test("statement Id, flags, and iteration count") {
    assert(cmd === Command.COM_STMT_EXECUTE)
    assert(id === stmtId)
    assert(flg === flags)
    assert(iter === iteration)
  }

  val len = ((params.size + 7) / 8).toInt
  val bytes = br.take(len)

  test("null bits") {
    val bytesAsBigEndian = bytes.reverse
    val bits = BigInt(bytesAsBigEndian)
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
      val raw = RawValue(Type.Timestamp, Charset.Binary, true, br.readLengthCodedBytes())
      val TimestampValue(ts) = raw
      assert(ts === timestamp)
    }

    test("java.sql.Date") {
      val raw = RawValue(Type.Date, Charset.Binary, true, br.readLengthCodedBytes())
      val DateValue(d) = raw
      assert(d.toString === sqlDate.toString)
    }

    test("java.util.Date") {
      val raw = RawValue(Type.DateTime, Charset.Binary, true, br.readLengthCodedBytes())
      val TimestampValue(dt) = raw
      assert(dt.getTime === datetime.getTime)
    }
  }
}