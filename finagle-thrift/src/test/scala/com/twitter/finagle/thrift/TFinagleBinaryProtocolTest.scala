package com.twitter.finagle.thrift

import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.thrift.Protocols.TFinagleBinaryProtocol
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TMemoryBuffer
import org.scalatest.BeforeAndAfter
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TFinagleBinaryProtocolTest extends AnyFunSuite with BeforeAndAfter with Matchers {

  private val NullCounter = NullStatsReceiver.counter("")

  private def assertSerializedBytes(expectedBytes: Array[Byte], trans: TMemoryBuffer): Unit = {
    // 4 bytes for the string length
    trans.length() should be(expectedBytes.length + 4)
    trans.getArray().drop(4).take(expectedBytes.length) should be(expectedBytes)
  }

  private def assertSerializedBytes(expected: String, trans: TMemoryBuffer): Unit = {
    val expectedBytes = expected.getBytes(StandardCharsets.UTF_8)
    assertSerializedBytes(expectedBytes, trans)
  }

  test("writeString") {
    val stats = new InMemoryStatsReceiver
    val largerThanTlOutBuffer = stats.counter("largerThanTlOutBuffer")
    val trans = new TMemoryBuffer(128)
    val proto = new TFinagleBinaryProtocol(
      trans,
      largerThanTlOutBuffer,
      stringLengthLimit = Protocols.NoLimit,
      containerLengthLimit = Protocols.NoLimit,
      strictRead = false,
      strictWrite = true)

    proto.writeString("abc")
    assertSerializedBytes("abc", trans)
    largerThanTlOutBuffer() should be(0)
  }

  test("writeString same as TBinaryProtocol") {
    def compare(str: String): Unit = {
      val plainTrans = new TMemoryBuffer(128)
      val plainProto = new TBinaryProtocol(plainTrans)

      val optTrans = new TMemoryBuffer(128)
      val optProto = new TFinagleBinaryProtocol(
        optTrans,
        NullCounter,
        stringLengthLimit = Protocols.NoLimit,
        containerLengthLimit = Protocols.NoLimit,
        strictRead = false,
        strictWrite = true)

      plainProto.writeString(str)
      optProto.writeString(str)
      plainTrans.length() should be(optTrans.length())
      plainTrans.getArray() should be(optTrans.getArray())
    }

    compare("wurmp wurmp!")
    compare("") // empty string path
    compare("\u2603\u2603\u2603\u2603")
  }

  test("writeString larger than threadlocal out buffer") {
    // todo: remove this test after dropping support for JDK8.
    assume(!TFinagleBinaryProtocol.usesCompactStrings)

    val longStr = new Random(214566).nextString(TFinagleBinaryProtocol.OutBufferSize + 1)

    val trans = new TMemoryBuffer(128)
    val stats = new InMemoryStatsReceiver
    val largerThanTlOutBuffer = stats.counter("largerThanTlOutBuffer")
    val proto = new TFinagleBinaryProtocol(
      trans,
      largerThanTlOutBuffer = largerThanTlOutBuffer,
      stringLengthLimit = Protocols.NoLimit,
      containerLengthLimit = Protocols.NoLimit,
      strictRead = false,
      strictWrite = true
    )
    proto.writeString(longStr)
    largerThanTlOutBuffer() should be(1)
    assertSerializedBytes(longStr, trans)
  }

  test("writeBinary handles non-zero arrayOffsets") {
    // todo: remove this test after dropping support for JDK8.
    assume(!TFinagleBinaryProtocol.usesCompactStrings)

    val len = 16
    val offset = 2

    val bbuf = ByteBuffer.allocate(len)
    0 until len foreach { i => bbuf.put(i.toByte) }
    bbuf.position(offset)
    val withOffset = bbuf.slice()
    withOffset.arrayOffset() should be(offset)

    val trans = new TMemoryBuffer(128)
    val proto = new TFinagleBinaryProtocol(
      trans,
      NullCounter,
      stringLengthLimit = Protocols.NoLimit,
      containerLengthLimit = Protocols.NoLimit,
      strictRead = false,
      strictWrite = true)
    proto.writeBinary(withOffset)

    val expected = bbuf.array().drop(offset)
    assertSerializedBytes(expected, trans)
  }

  test("writeBinary accepts read-only ByteBuffer's with offset and limit") {
    val len = 24
    val offset = 4
    val limit = 18

    val buffer = ByteBuffer.allocate(len)
    0.until(len).foreach { i => buffer.put(i.toByte) }
    buffer.position(offset)
    buffer.limit(limit)

    val trans = new TMemoryBuffer(128)
    val protocol = new TFinagleBinaryProtocol(
      trans,
      NullCounter,
      stringLengthLimit = Protocols.NoLimit,
      containerLengthLimit = Protocols.NoLimit,
      strictRead = false,
      strictWrite = true)
    protocol.writeBinary(buffer.asReadOnlyBuffer())

    val expected = buffer.array().drop(offset).take(limit - offset)
    assertSerializedBytes(expected, trans)
  }

  test("writeBinary accepts read-only ByteBuffer's") {
    val len = 24

    val buffer = ByteBuffer.allocate(len)
    0.until(len).foreach { i => buffer.put(i.toByte) }
    buffer.position(0)

    val trans = new TMemoryBuffer(128)
    val protocol = new TFinagleBinaryProtocol(
      trans,
      NullCounter,
      stringLengthLimit = Protocols.NoLimit,
      containerLengthLimit = Protocols.NoLimit,
      strictRead = false,
      strictWrite = true)
    protocol.writeBinary(buffer.asReadOnlyBuffer())

    val expected = buffer.array()
    assertSerializedBytes(expected, trans)
  }
}
