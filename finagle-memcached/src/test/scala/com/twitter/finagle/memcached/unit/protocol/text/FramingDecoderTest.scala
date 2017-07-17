package com.twitter.finagle.memcached.unit.protocol.text

import com.twitter.finagle.memcached.protocol.text.{FrameDecoder, FramingDecoder}
import com.twitter.io.Buf
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import scala.collection.mutable

class FramingDecoderTest extends FunSuite with MockitoSugar {

  private class MockFrameDecoder(private var rawSizes: List[Int]) extends FrameDecoder[Buf] {
    def nextFrameBytes(): Int = rawSizes match {
      case i::rs =>
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
    assert(framer(Buf.Utf8("STO")) == Seq.empty)
  }

  test("frame response without data") {
    val framer = newFramer(-1, -1) // second one because the framer checks to see if we need an empty Buf
    assert(framer(Buf.Utf8("STORED\r\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("accumulate partial response frame") {
    val framer = newFramer(-1, -1, -1, -1, -1)
    assert(framer(Buf.Utf8("ST")).isEmpty)
    assert(framer(Buf.Utf8("OR")).isEmpty)
    assert(framer(Buf.Utf8("ED\r")).isEmpty)
    assert(framer(Buf.Utf8("\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("accumulate response frame after returning frame") {
    val framer = newFramer(-1, -1, -1, -1, -1)
    framer(Buf.Utf8("ST"))
    assert(framer(Buf.Utf8("ORED\r\nNOT_ST")) == Seq(Buf.Utf8("STORED")))
    assert(framer(Buf.Utf8("ORED\r\n")) == Seq(Buf.Utf8("NOT_STORED")))
  }

  test("Frame multiple frames") {
    val framer = newFramer(-1, -1, -1)
    assert(framer(Buf.Utf8("STORED\r\nNOT_STORED\r\n")) ==
      Seq(Buf.Utf8("STORED"), Buf.Utf8("NOT_STORED")))
  }

  test("Frame data frame") {
    val framer = newFramer(
      -1, // initial line
      10, // after initial line, returns 10 but we have 0 bytes -> no decode
      10, // gets more data, asks again and gets 10 again -> can decode
      -1  // has zero data, but asks again in case the decoder wants a zero size buffer
    )
    assert(framer(Buf.Utf8("VALUE foo 0 10\r\n")) == Seq(Buf.Utf8("VALUE foo 0 10")))
    assert(framer(Buf.Utf8("abcdefghij\r\n")) == Seq(Buf.Utf8("abcdefghij")))
  }

  test("accumulate partial data frames") {
    val framer = newFramer(
      -1, // initial line
      10, // need 10 bytes. Only 3 available.
      10, // need 10 bytes. Only 6 available.
      10, // need 10 bytes. Only 9 available.
      10, // need 10 bytes. All 10 available.
      -1  // Back to text line
    )
    assert(framer(Buf.Utf8("VALUE foo 0 10\r\nabc")) == Seq(Buf.Utf8("VALUE foo 0 10")))
    framer(Buf.Utf8("def"))
    framer(Buf.Utf8("ghi"))
    assert(framer(Buf.Utf8("j\r\n")) == Seq(Buf.Utf8("abcdefghij")))
  }

  test("accumulate response after framing data frame") {
    val framer = newFramer(
      -1, // initial line
       3, // data frame
      -1, // text mode
      -1, // text mode for second half of text frame
      -1  // still text mode
    )
    assert(framer(Buf.Utf8("VALUE foo 0 3\r\nabc\r\nSTO")) ==
      Seq(Buf.Utf8("VALUE foo 0 3"), Buf.Utf8("abc")))
    assert(framer(Buf.Utf8("RED\r\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("Don't frame data frame until newlines are received") {
    val framer = newFramer(
      -1, // text mode
       3, // data mode 3 bytes, will have none.
       3, // still data mode: now we have 3 but not the line endings
       3, // still data mode: now we have 3 and the line endings
      -1  // text mode
    )

    framer(Buf.Utf8("VALUE foo 0 3\r\n"))
    assert(framer(Buf.Utf8("abc")) == Seq.empty)
    assert(framer(Buf.Utf8("\r\n")) == Seq(Buf.Utf8("abc")))
  }

  test("Ignore newlines in the middle of data frames") {
    val framer = newFramer(
      -1, // text mode
      10, // data mode, need 10. Will have 0.
      10, // still data mode. Have all the data now.
      -1  // back to text mode
    )
    framer(Buf.Utf8("VALUE foo 0 10\r\n"))
    assert(framer(Buf.Utf8("abc\r\ndef\r\n\r\n")) == Seq(Buf.Utf8("abc\r\ndef\r\n")))
  }
}
