package com.twitter.finagle.memcached.unit.protocol.text

import com.twitter.finagle.memcached.protocol.StorageCommand
import com.twitter.finagle.memcached.protocol.text.Framer
import com.twitter.finagle.memcached.protocol.text.server.ServerFramer
import com.twitter.io.{Buf, ByteReader}
import org.scalatest.funsuite.AnyFunSuite

class FramerTest extends AnyFunSuite {

  private class TestFramer extends ServerFramer(StorageCommand.StorageCommands)

  test("return empty frame sequence on partial frame") {
    val framer = new TestFramer
    assert(framer(Buf.Utf8("set")) == Seq.empty)
  }

  test("frame response without data") {
    val framer = new TestFramer
    assert(framer(Buf.Utf8("STORED\r\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("accumulate partial response frame") {
    val framer = new TestFramer
    assert(framer(Buf.Utf8("ST")).isEmpty)
    assert(framer(Buf.Utf8("OR")).isEmpty)
    assert(framer(Buf.Utf8("ED\r")).isEmpty)
    assert(framer(Buf.Utf8("\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("accumulate response frame after returning frame") {
    val framer = new TestFramer
    assert(framer(Buf.Utf8("ST")).isEmpty)
    assert(framer(Buf.Utf8("ORED\r\nNOT_ST")) == Seq(Buf.Utf8("STORED")))
    assert(framer(Buf.Utf8("ORED\r\n")) == Seq(Buf.Utf8("NOT_STORED")))
  }

  test("Frame multiple frames") {
    val framer = new TestFramer
    assert(
      framer(Buf.Utf8("STORED\r\nNOT_STORED\r\n")) ==
        Seq(Buf.Utf8("STORED"), Buf.Utf8("NOT_STORED"))
    )
  }

  test("Frame data frame") {
    val framer = new TestFramer
    assert(framer(Buf.Utf8("set foo 0 0 10\r\n")) == Seq(Buf.Utf8("set foo 0 0 10")))
    assert(framer(Buf.Utf8("abcdefghij\r\n")) == Seq(Buf.Utf8("abcdefghij")))
  }

  test("accumulate partial data frames") {
    val framer = new TestFramer
    assert(framer(Buf.Utf8("set foo 0 0 10\r\nabc")) == Seq(Buf.Utf8("set foo 0 0 10")))
    assert(framer(Buf.Utf8("def")).isEmpty)
    assert(framer(Buf.Utf8("ghi")).isEmpty)
    assert(framer(Buf.Utf8("j\r\n")) == Seq(Buf.Utf8("abcdefghij")))
  }

  test("accumulate response after framing data frame") {
    val framer = new TestFramer
    assert(
      framer(Buf.Utf8("set foo 0 0 3\r\nabc\r\nSTO")) ==
        Seq(Buf.Utf8("set foo 0 0 3"), Buf.Utf8("abc"))
    )
    assert(framer(Buf.Utf8("RED\r\n")) == Seq(Buf.Utf8("STORED")))
  }

  test("Don't frame data frame until newlines are received") {
    val framer = new TestFramer
    assert(framer(Buf.Utf8("set foo 0 0 3\r\n")) == Seq(Buf.Utf8("set foo 0 0 3")))
    assert(framer(Buf.Utf8("abc")) == Seq.empty)
    assert(framer(Buf.Utf8("\r\n")) == Seq(Buf.Utf8("abc")))
  }

  test("Ignore newlines in the middle of data frames") {
    val framer = new TestFramer
    assert(framer(Buf.Utf8("set foo 0 0 10\r\n")) == Seq(Buf.Utf8("set foo 0 0 10")))
    assert(framer(Buf.Utf8("abc\r\ndef\r\n\r\n")) == Seq(Buf.Utf8("abc\r\ndef\r\n")))
  }

  test("bytesBeforeLineEnd returns -1 on empty ByteReader") {
    val reader = ByteReader(Buf.Empty)
    assert(Framer.bytesBeforeLineEnd(reader) == -1)
  }

  test("bytesBeforeLineEnd returns 0 when reader's underlying buf starts with \r\n") {
    val reader = ByteReader(Buf.Utf8("\r\n"))
    assert(Framer.bytesBeforeLineEnd(reader) == 0)
  }

  test("bytesBeforeLineEnd returns -1 when reader's underling buf does not contain \r\n") {
    val reader = ByteReader(Buf.Utf8("foo bar baz"))
    assert(Framer.bytesBeforeLineEnd(reader) == -1)
  }

  test("bytesBeforeLineEnd returns index of \r\n in reader's underlying buf") {
    val reader = ByteReader(Buf.Utf8("foo \r\n bar"))
    assert(Framer.bytesBeforeLineEnd(reader) == 4)
  }
}
