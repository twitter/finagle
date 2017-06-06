package com.twitter.finagle.memcached.unit.protocol.text.client

import org.scalatest.FunSuite
import com.twitter.finagle.memcached.protocol
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.client.MemcachedClientDecoder
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf

class MemcachedClientDecoderTest extends FunSuite {

  test("parseResponse NOT_FOUND") {
    val context = new MemcachedClientDecoder

    val buffer = Buf.Utf8("NOT_FOUND")
    assert(context.decode(buffer) == NotFound)
  }

  test("parseResponse STORED") {
    val context = new MemcachedClientDecoder

    val buffer = Buf.Utf8("STORED")
    assert(context.decode(buffer) == Stored)
  }

  test("parseResponse EXISTS") {
    val context = new MemcachedClientDecoder

    val buffer = Buf.Utf8("EXISTS")
    assert(context.decode(buffer) == Exists)
  }

  test("parseResponse ERROR") {
    val context = new MemcachedClientDecoder

    val buffer = Buf.Utf8("ERROR")
    assert(context
      .decode(buffer).asInstanceOf[protocol.Error]
      .cause
      .getClass == classOf[NonexistentCommand])
  }

  test("parseResponse STATS") {
    val context = new MemcachedClientDecoder

    val line1 = Buf.Utf8("STAT items:1:number 1")
    val line2 = Buf.Utf8("STAT items:1:age 1468")
    val line3 = Buf.Utf8("ITEM foo [5 b; 1322514067 s]")
    val end = Buf.Utf8("END")

    val lines = Seq(line1, line2, line3)
    val tokens = lines.map(ParserUtils.splitOnWhitespace)

    assert(context.decode(line1) == null)
    assert(context.decode(line2) == null)
    assert(context.decode(line3) == null)
    val info = context.decode(end)

    assert(info.getClass == classOf[InfoLines])
    val ilines = info.asInstanceOf[InfoLines].lines
    assert(ilines.size == 3)
    ilines.zipWithIndex.foreach { case(line, idx) =>
      val key = tokens(idx).head
      val values = tokens(idx).drop(1)
      assert(line.key == key)
      assert(line.values.size == values.size)
      line.values.zipWithIndex.foreach { case(token, tokIdx) =>
        assert(token == values(tokIdx))
      }
    }
  }

  test("parseResponse CLIENT_ERROR") {
    val context = new MemcachedClientDecoder

    val errorMessage = "sad panda error"
    val buffer = Buf.Utf8(s"CLIENT_ERROR $errorMessage")
    val error = context.decode(buffer).asInstanceOf[protocol.Error]
    assert(error.cause.getClass == classOf[ClientError])
    assert(error.cause.getMessage() == errorMessage)
  }

  test("parseResponse SERVER_ERROR") {
    val context = new MemcachedClientDecoder

    val errorMessage = "sad panda error"
    val buffer = Buf.Utf8(s"SERVER_ERROR $errorMessage")
    val error = context.decode(buffer).asInstanceOf[protocol.Error]
    assert(error.cause.getClass == classOf[ServerError])
    assert(error.cause.getMessage() == errorMessage)
  }

}
