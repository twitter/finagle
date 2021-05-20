package com.twitter.finagle.memcached.unit.protocol.text

import com.twitter.finagle.memcached.protocol.text.{FrameDecoder, FramingDecoder}
import com.twitter.io.{Buf, ByteReader}
import org.scalatestplus.mockito.MockitoSugar
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class FramingDecoderTest extends AnyFunSuite with MockitoSugar {

  private class MockFrameDecoder(private var rawSizes: List[Int]) extends FrameDecoder[Buf] {
    def nextFrameBytes(): Int = rawSizes match {
      case i :: rs =>
        rawSizes = rs
        i

      case Nil => sys.error("No remaining sizes")
    }

    def decodeData(buf: Buf, results: mutable.Buffer[Buf]): Unit = results += buf
  }

  private def newFramer(sizes: Int*): FramingDecoder[Buf] =
    new FramingDecoder[Buf](new MockFrameDecoder(sizes.toList))

  test("return empty frame sequence on partial frame") {
    val framer = newFramer(-1)
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    framer(ByteReader(Buf.Utf8("STO")), outputMessages)
    assert(outputMessages.isEmpty)
  }

  test("frame response without data") {
    val framer =
      newFramer(-1, -1) // second one because the framer checks to see if we need an empty Buf
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    framer(ByteReader(Buf.Utf8("STORED\r\n")), outputMessages)
    assert(outputMessages == Seq(Buf.Utf8("STORED")))
  }

  test("don't incremement reader's reader index if frame not read") {
    val framer = newFramer(-1)
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    val reader = ByteReader(Buf.Utf8("STO"))
    framer(reader, outputMessages)
    assert(outputMessages.isEmpty)
    assert(reader.remaining == 3)
  }

  test("incremement reader's reader index after returning frame") {
    val framer = newFramer(-1, -1, -1, -1, -1)
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    val reader = ByteReader(Buf.Utf8("STORED\r\nNOT"))
    framer(reader, outputMessages)
    assert(outputMessages == Seq(Buf.Utf8("STORED")))
    assert(reader.remaining == 3)
  }

  test("Frame multiple frames") {
    val framer = newFramer(-1, -1, -1, -1)
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    framer(ByteReader(Buf.Utf8("STORED\r\nNOT_STORED\r\nSTORED\r\n")), outputMessages)
    assert(outputMessages == Seq(Buf.Utf8("STORED"), Buf.Utf8("NOT_STORED"), Buf.Utf8("STORED")))
  }

  test("Frame data frame") {
    val framer = newFramer(
      -1, // initial line
      10, // after initial line, returns 10 but we have 0 bytes -> no decode
      10, // gets more data, asks again and gets 10 again -> can decode
      -1 // has zero data, but asks again in case the decoder wants a zero size buffer
    )
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    framer(ByteReader(Buf.Utf8("VALUE foo 0 10\r\nabcdefghij\r\n")), outputMessages)
    assert(outputMessages == Seq(Buf.Utf8("VALUE foo 0 10"), Buf.Utf8("abcdefghij")))
  }

  test("frame data frame in second decode") {
    val framer = newFramer(
      -1, // initial line
      10, // need 10 bytes. Only 3 available.
      10, // need 10 bytes. Only 6 available.
      10, // need 10 bytes. Only 9 available.
      10, // need 10 bytes. All 10 available.
      -1 // Back to text line
    )
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    framer(ByteReader(Buf.Utf8("VALUE foo 0 10\r\nabc")), outputMessages)
    assert(outputMessages == Seq(Buf.Utf8("VALUE foo 0 10")))
    framer(ByteReader(Buf.Utf8("abcdefghij\r\n")), outputMessages)
    assert(outputMessages == Seq(Buf.Utf8("VALUE foo 0 10"), Buf.Utf8("abcdefghij")))
  }

  test("don't incremement reader's reader index if full data frame not read") {
    val framer = newFramer(
      -1, // initial line
      10 // need 10 bytes. Only 3 available.
    )
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    val reader = ByteReader(Buf.Utf8("VALUE foo 0 10\r\nabc"))
    framer(reader, outputMessages)
    assert(outputMessages == Seq(Buf.Utf8("VALUE foo 0 10")))
    assert(reader.remaining == 3)
  }

  test("Ignore newlines in the middle of data frames") {
    val framer = newFramer(
      -1, // text mode
      10, // data mode, need 10. Will have 0.
      10, // still data mode. Have all the data now.
      -1 // back to text mode
    )
    val outputMessages = new mutable.ArrayBuffer[Buf]()
    framer(ByteReader(Buf.Utf8("VALUE foo 0 10\r\nabc\r\ndef\r\n\r\n")), outputMessages)
    assert(outputMessages == Seq(Buf.Utf8("VALUE foo 0 10"), Buf.Utf8("abc\r\ndef\r\n")))
  }
}
