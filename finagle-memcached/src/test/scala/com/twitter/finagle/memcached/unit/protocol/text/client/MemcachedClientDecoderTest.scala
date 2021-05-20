package com.twitter.finagle.memcached.unit.protocol.text.client

import com.twitter.finagle.memcached.protocol
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.client.MemcachedClientDecoder
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class MemcachedClientDecoderTest extends AnyFunSuite {

  private class Context {
    val decoder = new MemcachedClientDecoder

    def decodeString(data: String): Seq[Response] =
      decodeBuf(Buf.Utf8(data))

    private def decodeBuf(data: Buf): Seq[Response] = {
      val out = new mutable.ArrayBuffer[Response]()
      decoder.decodeData(data, out)
      out.toSeq
    }
  }

  test("parseResponse NOT_FOUND") {
    assert(new Context().decodeString("NOT_FOUND") == Seq(NotFound))
  }

  test("parseResponse STORED") {
    assert(new Context().decodeString("STORED") == Seq(Stored))
  }

  test("parseResponse EXISTS") {
    assert(new Context().decodeString("EXISTS") == Seq(Exists))
  }

  test("parseResponse ERROR") {
    new Context().decodeString("ERROR") match {
      case Seq(protocol.Error(_: NonexistentCommand)) => assert(true)
      case other => fail(s"Unexpected output: $other")
    }
  }

  test("parse VALUE line") {
    val context = new Context
    assert(context.decodeString("VALUE key 0 0").isEmpty)
    assert(context.decoder.nextFrameBytes() == 0) // in data mode with size 0
    assert(context.decodeString("").isEmpty)
    assert(context.decoder.nextFrameBytes() == -1)
    context.decodeString("END") match {
      case Seq(Values(Seq(v))) =>
        assert(
          v == Value(
            key = Buf.Utf8("key"),
            value = Buf.Empty,
            casUnique = None,
            flags = Some(Buf.Utf8("0"))
          )
        )

      case other => fail(s"Unexpected output: $other")
    }
  }

  test("parse STATS") {
    val context = new Context

    val line1 = "STAT items:1:number 1"
    val line2 = "STAT items:1:age 1468"
    val line3 = "ITEM foo [5 b; 1322514067 s]"
    val end = "END"

    val lines = Seq(line1, line2, line3)
    val tokens = lines.map(str => ParserUtils.splitOnWhitespace(Buf.Utf8(str)))

    assert(context.decodeString(line1) == Seq.empty)
    assert(context.decoder.nextFrameBytes() == -1)
    assert(context.decodeString(line2) == Seq.empty)
    assert(context.decoder.nextFrameBytes() == -1)
    assert(context.decodeString(line3) == Seq.empty)
    assert(context.decoder.nextFrameBytes() == -1)

    context.decodeString(end) match {
      case Seq(InfoLines(ilines)) =>
        assert(ilines.size == 3)
        ilines.zipWithIndex.foreach {
          case (line, idx) =>
            val key = tokens(idx).head
            val values = tokens(idx).drop(1)
            assert(line.key == key)
            assert(line.values.size == values.size)
            line.values.zipWithIndex.foreach {
              case (token, tokIdx) =>
                assert(token == values(tokIdx))
            }
        }

      case other => fail(s"Unexpected value: $other")
    }
    assert(context.decoder.nextFrameBytes() == -1)
  }

  test("parseResponse CLIENT_ERROR") {

    val errorMessage = "sad panda error"
    new Context().decodeString(s"CLIENT_ERROR $errorMessage") match {
      case Seq(Error(cause)) => assert(cause.getMessage() == errorMessage)
      case other => fail(s"Unexpected output: $other")
    }
  }

  test("parseResponse SERVER_ERROR") {
    val errorMessage = "sad panda error"
    new Context().decodeString(s"SERVER_ERROR $errorMessage") match {
      case Seq(Error(cause: ServerError)) => assert(cause.getMessage() == errorMessage)
      case other => fail(s"Unexpected output: $other")
    }
  }
}
