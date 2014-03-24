package com.twitter.finagle.thrift

import com.google.common.base.Charsets
import com.twitter.finagle.stats.InMemoryStatsReceiver
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
class TFinagleBinaryProtocolTest extends FunSuite
  with BeforeAndAfter
  with ShouldMatchers
{

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
    val trans = new TMemoryBuffer(128)
    val proto = new TFinagleBinaryProtocol(trans, statsReceiver = stats)

    proto.writeString("abc")
    assertSerializedBytes("abc", trans)
    stats.counter("larger_than_threadlocal_out_buffer")() should be (0)
    stats.counter("fast_encode_failed")() should be (0)
  }

  test("writeString fallsback on encoding failure") {
    // use multi-byte chars so that we overflow on encode
    val str = new String(Array.fill(TFinagleBinaryProtocol.OutBufferSize) { '\u2603' })
    val byteLength = str.getBytes(Charsets.UTF_8).length
    str.length should be < byteLength
    byteLength should be > TFinagleBinaryProtocol.OutBufferSize

    val trans = new TMemoryBuffer(128)
    val stats = new InMemoryStatsReceiver
    val proto = new TFinagleBinaryProtocol(trans, statsReceiver = stats)
    proto.writeString(str)
    stats.counter("fast_encode_failed")() should be (1)
    assertSerializedBytes(str, trans)
  }

  test("writeString same as TBinaryProtocol") {
    def compare(str: String) {
      val plainTrans = new TMemoryBuffer(128)
      val plainProto = new TBinaryProtocol(plainTrans)

      val optTrans = new TMemoryBuffer(128)
      val optProto = new TFinagleBinaryProtocol(optTrans)

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
    val proto = new TFinagleBinaryProtocol(trans, statsReceiver = stats)
    proto.writeString(longStr)
    stats.counter("larger_than_threadlocal_out_buffer")() should be (1)
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
    val proto = new TFinagleBinaryProtocol(trans)
    proto.writeBinary(withOffset)

    val expected = bbuf.array().drop(offset)
    assertSerializedBytes(expected, trans)
  }

}
