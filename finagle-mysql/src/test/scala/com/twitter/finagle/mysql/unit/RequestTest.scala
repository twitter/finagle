package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.Parameter.NullParameter
import com.twitter.finagle.mysql.transport.MysqlBuf
import com.twitter.io.Buf
import java.sql.{Timestamp, Date => SQLDate}
import java.util.{Calendar, Date}
import org.scalatest.funsuite.AnyFunSuite

class SimpleCommandRequestTest extends AnyFunSuite {
  test("encode") {
    val bytes = "table".getBytes
    val cmd = 0x00
    val req = new SimpleCommandRequest(cmd.toByte, bytes)
    val br = MysqlBuf.reader(req.toPacket.body)
    assert(br.readByte() == cmd)
    assert(br.take(bytes.size) === bytes)
  }
}

class SslConnectionRequestTest extends AnyFunSuite {
  val clientCapabilities: Capability =
    Capability.baseCapabilities + Capability.ConnectWithDB + Capability.FoundRows
  val sslClientCapabilities: Capability = clientCapabilities + Capability.SSL
  val charset: Short = MysqlCharset.Utf8_general_ci
  val maxPacketSize: Int = 12345678

  test("Fails without SSL capability") {
    intercept[IllegalArgumentException] {
      SslConnectionRequest(clientCapabilities, charset, maxPacketSize)
    }
  }

  // The remaining tests for `SslConnectionRequest` are very similar
  // to the tests for the first part of `HandshakeResponse`.
  val req = SslConnectionRequest(sslClientCapabilities, charset, maxPacketSize)
  val br = MysqlBuf.reader(req.toPacket.body)

  test("encode capabilities") {
    val mask = br.readIntLE()
    assert(mask == 0x2ae8f)
  }

  test("maxPacketSize") {
    val max = br.readIntLE()
    assert(max == 12345678)
  }

  test("charset") {
    val charset = br.readByte()
    assert(charset == 33.toByte)
  }

  test("reserved bytes") {
    val rbytes = br.take(23)
    assert(rbytes.forall(_ == 0))
  }

}

abstract class HandshakeResponseTest extends AnyFunSuite {
  val username = Some("username")
  val password = Some("password")
  val database = Some("test")
  val salt =
    Array[Byte](70, 38, 43, 66, 74, 48, 79, 126, 76, 66, 70, 118, 67, 40, 63, 68, 120, 80, 103, 54)
  val maxPacketSize = 16777216

  protected def clientCapabilities(): Capability = Capability(0xfffff6ff)
  protected def serverCapabilities(): Capability = Capability(0xf7ff)
  protected def createHandshakeResponse(): HandshakeResponse

  val req = createHandshakeResponse()
  val packet = req.toPacket
  val br = MysqlBuf.reader(packet.body)

  test("encode capabilities") {
    val mask = br.readIntLE()
    assert(mask == clientCapabilities().mask)
  }

  test("maxPacketSize") {
    val max = br.readIntLE()
    assert(max == maxPacketSize)
  }

  test("charset") {
    val charset = br.readByte()
    assert(charset == 33.toByte)
  }

  test("reserved bytes") {
    val rbytes = br.take(23)
    assert(rbytes.forall(_ == 0))
  }

  test("username") {
    assert(br.readNullTerminatedString() == username.get)
  }

  test("password") {
    assert(br.readLengthCodedBytes() === req.hashPassword)
  }
}

class PlainHandshakeResponseTest extends HandshakeResponseTest {
  protected def createHandshakeResponse(): HandshakeResponse =
    PlainHandshakeResponse(
      username,
      password,
      database,
      clientCapabilities(),
      salt,
      serverCapabilities(),
      MysqlCharset.Utf8_general_ci,
      maxPacketSize,
      enableCachingSha2PasswordAuth = false
    )
}

class SecureHandshakeResponseTest extends HandshakeResponseTest {

  override protected def serverCapabilities(): Capability =
    Capability.baseCapabilities + Capability.SSL
  override protected def clientCapabilities(): Capability =
    Capability.baseCapabilities + Capability.ConnectWithDB + Capability.FoundRows + Capability.SSL

  protected def createHandshakeResponse(): HandshakeResponse =
    SecureHandshakeResponse(
      username,
      password,
      database,
      clientCapabilities(),
      salt,
      serverCapabilities(),
      MysqlCharset.Utf8_general_ci,
      maxPacketSize,
      enableCachingSha2PasswordAuth = false
    )

  test("Fails without client SSL capability") {
    intercept[IllegalArgumentException] {
      SecureHandshakeResponse(
        username,
        password,
        database,
        clientCapabilities() - Capability.SSL,
        salt,
        serverCapabilities(),
        MysqlCharset.Utf8_general_ci,
        maxPacketSize,
        enableCachingSha2PasswordAuth = false
      )
    }
  }

  test("Fails without server SSL capability") {
    intercept[IllegalArgumentException] {
      SecureHandshakeResponse(
        username,
        password,
        database,
        clientCapabilities(),
        salt,
        serverCapabilities() - Capability.SSL,
        MysqlCharset.Utf8_general_ci,
        maxPacketSize,
        enableCachingSha2PasswordAuth = false
      )
    }
  }
}

class AuthSwitchResponseTest extends AnyFunSuite {
  val password = Some("password")
  val salt =
    Array[Byte](70, 38, 43, 66, 74, 48, 79, 126, 76, 66, 70, 118, 67, 40, 63, 68, 120, 80, 103, 54)
  val charset = 255.toShort

  val req = AuthSwitchResponse(seqNum = 1.toShort, password, salt, charset, withSha256 = false)
  val br = MysqlBuf.reader(req.toPacket.body)

  test("encode password") {
    assert(br.readNullTerminatedBytes() === req.hashPassword)
  }
}

class AuthMoreDataToServerTest extends AnyFunSuite {
  test("AuthMoreData request public key") {
    val req = PlainAuthMoreDataToServer(seqNum = 1.toShort, NeedPublicKey)
    val br = MysqlBuf.reader(req.toPacket.body)

    assert(br.readByte() == NeedPublicKey.moreDataByte)
  }

  test("AuthMoreData fast auth success") {
    val req = PlainAuthMoreDataToServer(seqNum = 1.toShort, FastAuthSuccess)
    val br = MysqlBuf.reader(req.toPacket.body)

    assert(br.readByte() == FastAuthSuccess.moreDataByte)
  }

  test("AuthMoreData perform full auth") {
    val req = PlainAuthMoreDataToServer(seqNum = 1.toShort, PerformFullAuth)
    val br = MysqlBuf.reader(req.toPacket.body)

    assert(br.readByte == PerformFullAuth.moreDataByte)
  }

  test("AuthMoreData with password auth data") {
    val authData = Array[Byte](70, 38, 43, 66, 74, 48, 79, 126, 76, 66)
    val req = PasswordAuthMoreDataToServer(seqNum = 1.toShort, PerformFullAuth, authData)
    val br = MysqlBuf.reader(req.toPacket.body)

    assert(Buf.ByteArray.Owned.extract(br.readBytes(10)).sameElements(authData))
  }
}

class ExecuteRequestTest extends AnyFunSuite {
  test("null values") {
    val numOfParams = 18
    val nullParams: Array[Parameter] = Array.fill(numOfParams)(null)
    val e = ExecuteRequest(0, nullParams, false)
    val br = MysqlBuf.reader(e.toPacket.body)
    br.skip(10) // payload header (10 bytes)
    br.skip(1) // new params bound flag
    assert(br.remaining == ((numOfParams + 7) / 8))
  }

  // supported types
  val strVal = "test"
  val nonAsciiStrVal = "バイトルドットコム"
  val boolVal = true
  val byteVal = 1.toByte
  val shortVal = 2.toShort
  val intVal = 3
  val longVal = 4L
  val floatVal = 1.5f
  val doubleVal = 2.345
  val cal = Calendar.getInstance()
  val millis = cal.getTimeInMillis
  val timestamp = new Timestamp(millis)
  val sqlDate = new SQLDate(millis)
  val datetime = new Date(millis)
  val params: IndexedSeq[Parameter] = IndexedSeq(
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
    null,
    StringValue(strVal),
    ByteValue(byteVal),
    ShortValue(shortVal),
    IntValue(intVal),
    LongValue(longVal),
    FloatValue(floatVal),
    DoubleValue(doubleVal),
    null
  )
  // create a prepared statement
  val stmtId = 1
  val flags = 0
  val req = ExecuteRequest(stmtId, params)
  val br = MysqlBuf.reader(req.toPacket.body)

  val cmd = br.readByte()
  val id = br.readIntLE()
  val flg = br.readByte()
  val iter = br.readIntLE()
  test("statement Id, flags, and iteration count") {
    assert(cmd == Command.COM_STMT_EXECUTE)
    assert(id == stmtId)
    assert(flg == flags)
    assert(iter == 1)
  }

  val len = ((params.size + 7) / 8).toInt
  val bytes = br.take(len)

  test("null bits") {
    val bytesAsBigEndian = bytes.reverse
    val bits = BigInt(bytesAsBigEndian)
    for (i <- 0 until params.size) {
      if (params(i) == null)
        assert(bits.testBit(i) == true)
      else
        assert(bits.testBit(i) == false)
    }
  }

  val hasNewParams = br.readByte() == 1
  test("has new parameters") {
    assert(hasNewParams == true)
  }

  test("sanitized null parameters") {
    assert(!req.params.contains(null))
    assert(req.params.count(_ == NullParameter) == 4)
  }

  if (hasNewParams) {
    test("type codes") {
      for (p <- req.params)
        assert(br.readShortLE() == p.typeCode)
    }

    test("String") {
      assert(br.readLengthCodedString(MysqlCharset.defaultCharset) == strVal)
    }

    test("Non-Ascii String") {
      assert(br.readLengthCodedString(MysqlCharset.defaultCharset) == nonAsciiStrVal)
    }

    test("Boolean") {
      assert(br.readByte() == (if (boolVal) 1 else 0))
    }

    test("Byte") {
      assert(br.readByte() == byteVal)
    }

    test("Short") {
      assert(br.readShortLE() == shortVal)
    }

    test("Int") {
      assert(br.readIntLE() == intVal)
    }

    test("Long") {
      assert(br.readLongLE() == longVal)
    }

    test("Float") {
      assert(br.readFloatLE() == floatVal)
    }

    test("Double") {
      assert(br.readDoubleLE() == doubleVal)
    }

    test("java.sql.Timestamp") {
      val raw = RawValue(Type.Timestamp, MysqlCharset.Binary, true, br.readLengthCodedBytes())
      val TimestampValue(ts) = raw
      assert(ts == timestamp)
    }

    test("java.sql.Date") {
      val raw = RawValue(Type.Date, MysqlCharset.Binary, true, br.readLengthCodedBytes())
      val DateValue(d) = raw
      assert(d.toString == sqlDate.toString)
    }

    test("java.util.Date") {
      val raw = RawValue(Type.DateTime, MysqlCharset.Binary, true, br.readLengthCodedBytes())
      val TimestampValue(dt) = raw
      assert(dt.getTime == timestamp.getTime)
    }

    test("StringValue") {
      assert(br.readLengthCodedString(MysqlCharset.defaultCharset) == strVal)
    }

    test("ByteValue") {
      assert(br.readByte() == byteVal)
    }

    test("ShortValue") {
      assert(br.readShortLE() == shortVal)
    }

    test("IntValue") {
      assert(br.readIntLE() == intVal)
    }

    test("LongValue") {
      assert(br.readLongLE() == longVal)
    }

    test("FloatValue") {
      assert(br.readFloatLE() == floatVal)
    }

    test("DoubleValue") {
      assert(br.readDoubleLE() == doubleVal)
    }
  }
}
