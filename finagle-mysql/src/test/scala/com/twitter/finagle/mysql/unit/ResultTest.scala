package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.Packet
import com.twitter.io.Buf
import org.scalatest.funsuite.AnyFunSuite

trait HexDump {
  val hex: String
  // parse multi-line hex dump where packets are delimited by '|'
  lazy val packets: Seq[Packet] = {
    val tokens = hex.stripMargin
      .split('|')
      .map(_.replace("\n", " "))
      .map(s => s.split(' ').filterNot(_ == ""))
    val asBytes = tokens.map(_.map(s => ((s(0).asDigit << 4) + s(1).asDigit).toByte))
    asBytes.map(arr => Packet(arr(3), Buf.ByteArray.Owned(arr.drop(4))))
  }
}

class HandshakeInitTest extends AnyFunSuite {
  val authPluginHex =
    test("decode protocol version 10")(new HexDump {
      val hex =
        """36 00 00 00 0a 35 2e 35    2e 32 2d 6d 32 00 0b 00
        |00 00 64 76 48 40 49 2d    43 4a 00 ff f7 21 02 00
        |00 00 00 00 00 00 00 00    00 00 00 00 00 2a 34 64
        |7c 63 5a 77 6b 34 5e 5d    3a 00"""
      assert(packets.size > 0)
      val h = HandshakeInit.decode(packets(0))
      assert(h.protocol == 10)
      assert(h.version == "5.5.2-m2")
      assert(h.threadId == 11)
      assert(h.serverCapabilities.mask == 0xf7ff)
      assert(h.charset == MysqlCharset.Utf8_general_ci)
      assert(h.status == 2)
      assert(h.salt.length == 20)
      assert(
        h.salt === Array[Byte](100, 118, 72, 64, 73, 45, 67, 74, 42, 52, 100, 124, 99, 90, 119, 107,
          52, 94, 93, 58)
      )
    })

  test("decode protocol version 10 with auth plugin name")(new HexDump {
    val hex =
      """50 00 00 00 0a 35 2e 36    2e 34 2d 6d 37 2d 6c 6f
        |67 00 56 0a 00 00 52 42    33 76 7a 26 47 72 00 ff
        |ff 08 02 00 0f c0 15 00    00 00 00 00 00 00 00 00
        |00 2b 79 44 26 2f 5a 5a    33 30 35 5a 47 00 6d 79
        |73 71 6c 5f 6e 61 74 69    76 65 5f 70 61 73 73 77
        |6f 72 64 00"""
    assert(packets.size > 0)
    val h = HandshakeInit.decode(packets(0))
    assert(h.protocol == 10)
    assert(h.version == "5.6.4-m7-log")
    assert(h.threadId == 2646)
    assert(MysqlCharset.isLatin1(h.charset))
    assert(h.serverCapabilities.has(Capability.Protocol41))
    assert(h.serverCapabilities.has(Capability.PluginAuth))
    assert(h.serverCapabilities.has(Capability.SecureConnection))
    assert(h.status == 2)
    assert(h.salt.length == 20)
  })
}

class AuthSwitchRequestTest extends AnyFunSuite with HexDump {
  val hex =
    """2C 00 00 02 FE 63 61 63    68 69 6E 67 5F 73 68 61
      |32 5F 70 61 73 73 77 6F    72 64 00 0F 71 2E 02 5B
      |17 57 63 6B 19 4E 47 6A    0E 5B 01 40 72 26 41 00""".stripMargin
  val pluginData =
    Array(15, 113, 46, 2, 91, 23, 87, 99, 107, 25, 78, 71, 106, 14, 91, 1, 64, 114, 38, 65)

  test("decode") {
    assert(packets.size > 0)
    val authSwitchRequest = AuthSwitchRequest.decode(packets.head)
    assert(authSwitchRequest.seqNum == 2)
    assert(authSwitchRequest.pluginName == "caching_sha2_password")
    assert(authSwitchRequest.pluginData.sameElements(pluginData))
  }
}

class AuthMoreDataFromServerTest extends AnyFunSuite {

  test("fast auth success packet")(new HexDump {
    val hex = """02 00 00 04 01 03"""

    assert(packets.size > 0)
    val authMoreData = AuthMoreDataFromServer.decode(packets.head)
    assert(authMoreData.seqNum == 4)
    assert(authMoreData.moreDataType == FastAuthSuccess)
    assert(authMoreData.authData.isEmpty)
  })

  test("perform full auth packet")(new HexDump {
    val hex = """02 00 00 04 01 04"""

    assert(packets.size > 0)
    val authMoreData = AuthMoreDataFromServer.decode(packets.head)
    assert(authMoreData.seqNum == 4)
    assert(authMoreData.moreDataType == PerformFullAuth)
    assert(authMoreData.authData.isEmpty)
  })

  test("auth more data with RSA public key")(new HexDump {
    val hex =
      """11 01 00 06 01 2D 2D 2D    2D 2D 42 45 47 49 4E 20
        |50 55 42 4C 49 43 20 4B    45 59 2D 2D 2D 2D 2D 0A
        |4D 49 47 66 4D 41 30 47    43 53 71 47 53 49 62 33
        |44 51 45 42 41 51 55 41    41 34 47 4E 41 44 43 42
        |69 51 4B 42 67 51 44 42    7A 6F 71 51 75 77 5A 39
        |2F 65 4B 4D 4F 55 30 4F    58 57 4A 34 49 53 6A 2B
        |0A 32 59 45 6B 38 6C 34    4F 6D 42 6F 43 39 59 32
        |77 4E 45 65 34 50 63 6A    7A 6D 43 46 2F 66 39 61
        |5A 44 79 48 36 7A 6E 68    30 47 36 67 6D 62 2F 79
        |72 54 76 75 4E 4C 59 6B    54 55 67 69 46 4E 6D 30
        |79 0A 4A 32 72 53 7A 6C    67 6D 4A 5A 48 6B 57 79
        |6B 52 6B 6A 4B 72 34 56    30 34 69 41 61 48 64 55
        |34 4F 52 72 65 37 4D 73    39 65 6C 6E 37 6B 38 43
        |65 56 51 46 70 43 6A 4D    35 31 48 4F 4C 6B 70 38
        |49 68 0A 6E 41 56 72 6B    4F 68 53 62 48 49 34 76
        |78 70 72 62 51 49 44 41    51 41 42 0A 2D 2D 2D 2D
        |2D 45 4E 44 20 50 55 42    4C 49 43 20 4B 45 59 2D
        |2D 2D 2D 2D 0A""".stripMargin

    assert(packets.size > 0)
    val authMoreData = AuthMoreDataFromServer.decode(packets.head)
    assert(authMoreData.seqNum == 6)
    assert(authMoreData.moreDataType == NeedPublicKey)
    assert(authMoreData.authData.isDefined)
  })
}

class OKTest extends AnyFunSuite with HexDump {
  val hex = """07 00 00 02 00 00 00 02    00 00 00"""
  test("decode") {
    assert(packets.size > 0)
    val ok = OK.decode(packets(0))
    assert(ok.affectedRows == 0x00)
    assert(ok.insertId == 0x00)
    assert(ok.serverStatus == 0x02)
    assert(ok.warningCount == 0x00)
    assert(ok.message.isEmpty)
  }
}

class ErrorTest extends AnyFunSuite with HexDump {
  val hex =
    """17 00 00 01 ff 48 04 23    48 59 30 30 30 4e 6f 20
      |74 61 62 6c 65 73 20 75    73 65 64"""
  test("decode") {
    assert(packets.size > 0)
    val error = Error.decode(packets(0))
    assert(error.code == 0x0448)
    assert(error.sqlState == "#HY000")
    println(error.message)
    assert(error.message == "No tables used")
  }
}

class EofTest extends AnyFunSuite with HexDump {
  val hex = """05 00 00 05 fe 00 00 02 00"""
  test("decode") {
    assert(packets.size > 0)
    val eof = EOF.decode(packets(0))
    assert(eof.warnings == 0x00)
    assert(eof.serverStatus.mask == 0x02)
  }
}

class PrepareOKTest extends AnyFunSuite with HexDump {
  // SELECT CONCAT(?, ?) AS col1:
  val hex =
    """0c 00 00 01 00 01 00 00    00 01 00 02 00 00 00 00|
      |17 00 00 02 03 64 65 66    00 00 00 01 3f 00 0c 3f
      |00 00 00 00 00 fd 80 00    00 00 00|17 00 00 03 03
      |64 65 66 00 00 00 01 3f    00 0c 3f 00 00 00 00 00
      |fd 80 00 00 00 00|05 00    00 04 fe 00 00 02 00|1a
      |00 00 05 03 64 65 66 00    00 00 04 63 6f 6c 31 00
      |0c 3f 00 00 00 00 00 fd    80 00 1f 00 00|05 00 00
      |06 fe 00 00 02 00"""
  test("decode") {
    assert(packets.size > 0, "expected header packet")
    val p = PrepareOK.decode(packets.head)
    assert(p.id == 1)
    assert(p.numOfParams == 2)
    assert(p.numOfCols == 1)
    assert(packets.size >= 1 + p.numOfParams, "expected %d param packets".format(p.numOfParams))
    val params = packets
      .drop(1) /*drop header*/
      .take(p.numOfParams)
      .map(Field.decode(_))
    val p1 = params(0)
    val p2 = params(1)
    assert(p1.name == "?")
    assert(p1.fieldType == Type.VarString)
    assert(p1.charset == MysqlCharset.Binary)
    assert(p2.name == "?")
    assert(p2.fieldType == Type.VarString)
    assert(p2.charset == MysqlCharset.Binary)
    assert(
      packets.size >= 1 + p.numOfParams + p.numOfCols,
      "expected %d column packets".format(p.numOfCols)
    )
    val cols = packets
      .drop(2 + p.numOfParams) /*drop header + eof + params*/
      .take(p.numOfCols)
      .map(Field.decode(_))
    val col = cols(0)
    assert(col.name == "col1")
  }
}

class BinaryResultSetTest extends AnyFunSuite with HexDump {
  // SELECT CONCAT(?, ?) AS col1
  // execute("foo", "bar")
  val hex =
    """01 00 00 01 01|1a 00 00    02 03 64 65 66 00 00 00
      |04 63 6f 6c 31 00 0c 08    00 06 00 00 00 fd 00 00
      |1f 00 00|05 00 00 03 fe    00 00 02 00|09 00 00 04
      |00 00 06 66 6f 6f 62 61    72|05 00 00 05 fe 00 00
      |02 00"""
  test("decode") {
    assert(packets.size == 5, "expected at least 5 packet")
    val rs = ResultSetBuilder.decode(isBinaryEncoded = true, supportUnsigned = false)(
      packets.head,
      packets.drop(1).take(1), /* column_count = 1 */
      packets.drop(3).take(1) /* drop eof, 1 row */
    )
    assert(rs.fields.size == 1)
    assert(rs.rows.size == 1)
    assert(rs.fields(0).name == "col1")
    rs.rows(0)("col1") match {
      case Some(StringValue(s)) => assert(s == "foobar")
      case v => fail("expected StringValue(foobar), but got %s".format(v))
    }
  }
}
