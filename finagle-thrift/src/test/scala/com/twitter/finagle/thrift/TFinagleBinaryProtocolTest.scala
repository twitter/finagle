package com.twitter.finagle.thrift

import com.google.common.base.Charsets
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.thrift.Protocols.TFinagleBinaryProtocol
import java.nio.ByteBuffer
import org.apache.thrift.transport.TMemoryBuffer
import org.apache.thrift.protocol.TBinaryProtocol
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TFinagleBinaryProtocolTest extends FunSuite with BeforeAndAfter with ShouldMatchers {

  private val NullCounter = NullStatsReceiver.counter("")

  private def assertSerializedBytes(
    expectedBytes: Array[Byte],
    trans: TMemoryBuffer
  ) {
    // 4 bytes for the string length
    trans.length() should be (expectedBytes.length + 4)
    trans.getArray().drop(4).take(expectedBytes.length) should be (expectedBytes)
  }

  private def assertSerializedBytes(
    expected: String,
    trans: TMemoryBuffer
  ) {
    val expectedBytes = expected.getBytes(Charsets.UTF_8)
    assertSerializedBytes(expectedBytes, trans)
  }

  test("writeString") {
    val stats = new InMemoryStatsReceiver
    val fastEncodeFailed = stats.counter("fastEncodeFailed")
    val largerThanTlOutBuffer = stats.counter("largerThanTlOutBuffer")
    val trans = new TMemoryBuffer(128)
    val proto = new TFinagleBinaryProtocol(trans, fastEncodeFailed, largerThanTlOutBuffer)

    proto.writeString("abc")
    assertSerializedBytes("abc", trans)
    fastEncodeFailed() should be (0)
    largerThanTlOutBuffer() should be (0)
  }

  test("writeString fallsback on encoding failure") {
    // use multi-byte chars so that we overflow on encode
    val str = new String(Array.fill(TFinagleBinaryProtocol.OutBufferSize) { '\u2603' })
    val byteLength = str.getBytes(Charsets.UTF_8).length
    str.length should be < byteLength
    byteLength should be > TFinagleBinaryProtocol.OutBufferSize

    val trans = new TMemoryBuffer(128)
    val stats = new InMemoryStatsReceiver
    val fastEncodeFailed = stats.counter("fastEncodeFailed")
    val proto = new TFinagleBinaryProtocol(
      trans, fastEncodeFailed = fastEncodeFailed, largerThanTlOutBuffer = NullCounter)
    proto.writeString(str)
    fastEncodeFailed() should be (1)
    assertSerializedBytes(str, trans)
  }

  test("writeString same as TBinaryProtocol") {
    def compare(str: String) {
      val plainTrans = new TMemoryBuffer(128)
      val plainProto = new TBinaryProtocol(plainTrans)

      val optTrans = new TMemoryBuffer(128)
      val optProto = new TFinagleBinaryProtocol(optTrans, NullCounter, NullCounter)

      plainProto.writeString(str)
      optProto.writeString(str)
      plainTrans.length() should be (optTrans.length())
      plainTrans.getArray() should be (optTrans.getArray())
    }

    compare("wurmp wurmp!")
    compare("") // empty string path
    compare("\u2603\u2603\u2603\u2603")
  }

  test("writeString larger than threadlocal out buffer") {
    val longStr = new Random(214566).nextString(TFinagleBinaryProtocol.OutBufferSize + 1)

    val trans = new TMemoryBuffer(128)
    val stats = new InMemoryStatsReceiver
    val largerThanTlOutBuffer = stats.counter("largerThanTlOutBuffer")
    val proto = new TFinagleBinaryProtocol(
      trans, fastEncodeFailed = NullCounter, largerThanTlOutBuffer = largerThanTlOutBuffer)
    proto.writeString(longStr)
    largerThanTlOutBuffer() should be (1)
    assertSerializedBytes(longStr, trans)
  }

  test("writeBinary handles non-zero arrayOffsets") {
    val len = 16
    val offset = 2

    val bbuf = ByteBuffer.allocate(len)
    0 until len foreach { i => bbuf.put(i.toByte) }
    bbuf.position(offset)
    val withOffset = bbuf.slice()
    withOffset.arrayOffset() should be (offset)

    val trans = new TMemoryBuffer(128)
    val proto = new TFinagleBinaryProtocol(trans, NullCounter, NullCounter)
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
    val protocol = new TFinagleBinaryProtocol(trans, NullCounter, NullCounter)
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
    val protocol = new TFinagleBinaryProtocol(trans, NullCounter, NullCounter)
    protocol.writeBinary(buffer.asReadOnlyBuffer())

    val expected = buffer.array()
    assertSerializedBytes(expected, trans)
  }
}
