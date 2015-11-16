package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader, Packet}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait HexDump {
  val hex: String
  // parse multi-line hex dump where packets are delimited by '|'
  lazy val packets: Seq[Packet] = {
    val tokens = hex.stripMargin.split('|').map(_.replace("\n", " "))
      .map(s => s.split(' ').filterNot(_ == ""))
    val asBytes = tokens.map(_.map(s =>
      ((s(0).asDigit << 4) + s(1).asDigit).toByte))
    asBytes.map(arr => Packet(arr(3), Buffer(arr.drop(4))))
  }
}

@RunWith(classOf[JUnitRunner])
class HandshakeInitTest extends FunSuite {
  val authPluginHex =

  test("decode protocol version 10") (new HexDump {
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
    assert(h.serverCap.mask == 0xf7ff)
    assert(h.charset == Charset.Utf8_general_ci)
    assert(h.status == 2)
    assert(h.salt.length == 20)
    assert(h.salt === Array[Byte](
      100, 118, 72, 64, 73, 45, 67, 74,
      42, 52, 100, 124, 99, 90, 119, 107,
      52, 94, 93, 58))
  })

  test("decode protocol version 10 with auth plugin name") (new HexDump {
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
    assert(Charset.isLatin1(h.charset))
    assert(h.serverCap.has(Capability.Protocol41))
    assert(h.serverCap.has(Capability.PluginAuth))
    assert(h.serverCap.has(Capability.SecureConnection))
    assert(h.status == 2)
    assert(h.salt.length == 20)
  })
}

@RunWith(classOf[JUnitRunner])
class OKTest extends FunSuite with HexDump {
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

@RunWith(classOf[JUnitRunner])
class ErrorTest extends FunSuite with HexDump {
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

@RunWith(classOf[JUnitRunner])
class EofTest extends FunSuite with HexDump {
  val hex = """05 00 00 05 fe 00 00 02 00"""
  test("decode") {
    assert(packets.size > 0)
    val eof = EOF.decode(packets(0))
    assert(eof.warnings == 0x00)
    assert(eof.serverStatus == 0x02)
  }
}

@RunWith(classOf[JUnitRunner])
class PrepareOKTest extends FunSuite with HexDump {
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
    val params = packets.drop(1) /*drop header*/
      .take(p.numOfParams).map(Field.decode(_))
    val p1 = params(0)
    val p2 = params(1)
    assert(p1.name == "?")
    assert(p1.fieldType == Type.VarString)
    assert(p1.charset == Charset.Binary)
    assert(p2.name == "?")
    assert(p2.fieldType == Type.VarString)
    assert(p2.charset == Charset.Binary)
    assert(packets.size >= 1 + p.numOfParams + p.numOfCols, "expected %d column packets".format(p.numOfCols))
    val cols = packets.drop(2 + p.numOfParams) /*drop header + eof + params*/
      .take(p.numOfCols).map(Field.decode(_))
    val col = cols(0)
    assert(col.name == "col1")
  }
}

@RunWith(classOf[JUnitRunner])
class BinaryResultSetTest extends FunSuite with HexDump {
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
    val rs = ResultSet.decode(true)(packets.head,
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

