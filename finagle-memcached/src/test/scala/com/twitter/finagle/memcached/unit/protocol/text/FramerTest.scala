package com.twitter.finagle.memcached.unit.protocol.text

import com.twitter.finagle.memcached.protocol.text.client.ClientFramer
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FramerTest extends FunSuite {

  test("return empty frame sequence on partial frame") {
    val framer = new ClientFramer
    assert(framer(Buf.Utf8("STO")) == Seq.empty)
  }

  test("frame response without data") {
    val framer = new ClientFramer
    assert(framer(Buf.Utf8("STORED\r\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("accumulate partial response frame") {
    val framer = new ClientFramer
    framer(Buf.Utf8("ST"))
    framer(Buf.Utf8("OR"))
    framer(Buf.Utf8("ED\r"))
    assert(framer(Buf.Utf8("\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("accumulate response frame after returning frame") {
    val framer = new ClientFramer
    framer(Buf.Utf8("ST"))
    assert(framer(Buf.Utf8("ORED\r\nNOT_ST")) == Seq(Buf.Utf8("STORED")))
    assert(framer(Buf.Utf8("ORED\r\n")) == Seq(Buf.Utf8("NOT_STORED")))
  }

  test("Frame multiple frames") {
    val framer = new ClientFramer
    assert(framer(Buf.Utf8("STORED\r\nNOT_STORED\r\n")) ==
      Seq(Buf.Utf8("STORED"), Buf.Utf8("NOT_STORED")))
  }

  test("Frame data frame") {
    val framer = new ClientFramer
    assert(framer(Buf.Utf8("VALUE foo 0 10\r\n")) == Seq(Buf.Utf8("VALUE foo 0 10")))
    assert(framer(Buf.Utf8("abcdefghij\r\n")) == Seq(Buf.Utf8("abcdefghij")))
  }

  test("accumulate partial data frames") {
    val framer = new ClientFramer
    assert(framer(Buf.Utf8("VALUE foo 0 10\r\nabc")) == Seq(Buf.Utf8("VALUE foo 0 10")))
    framer(Buf.Utf8("def"))
    framer(Buf.Utf8("ghi"))
    assert(framer(Buf.Utf8("j\r\n")) == Seq(Buf.Utf8("abcdefghij")))
  }

  test("accumulate response after framing data frame") {
    val framer = new ClientFramer
    assert(framer(Buf.Utf8("VALUE foo 0 3\r\nabc\r\nSTO")) ==
      Seq(Buf.Utf8("VALUE foo 0 3"), Buf.Utf8("abc")))
    assert(framer(Buf.Utf8("RED\r\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("Don't frame data frame until newlines are received") {
    val framer = new ClientFramer
    framer(Buf.Utf8("VALUE foo 0 3\r\n"))
    assert(framer(Buf.Utf8("abc")) == Seq.empty)
    assert(framer(Buf.Utf8("\r\n")) == Seq(Buf.Utf8("abc")))
  }

  test("Ignore newlines in the middle of data frames") {
    val framer = new ClientFramer
    framer(Buf.Utf8("VALUE foo 0 10\r\n"))
    assert(framer(Buf.Utf8("abc\r\ndef\r\n\r\n")) == Seq(Buf.Utf8("abc\r\ndef\r\n")))
  }
}