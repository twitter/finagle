package com.twitter.finagle.memcached.unit.protocol.text.client

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import com.twitter.finagle.memcached.protocol.text.client.Decoder
import com.twitter.finagle.memcached.protocol.text.{TokensWithData, ValueLines, Tokens, StatLines}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.io.Buf

@RunWith(classOf[JUnitRunner])
class DecoderTest extends FunSuite with MockitoSugar {

  class Context {
    val decoder = new Decoder
    decoder.start()
  }

  test("decode tokens with full delimiter") {
    val context = new Context
    import context._

    val buffer = "STORED\r\n"
    assert(decoder.decode(null, null, buffer) == Tokens(Seq(Buf.Utf8("STORED"))))
  }

  test("decode tokens with partial delimiter") {
    val context = new Context
    import context._

    val buffer = "STORED\r"
    assert(decoder.decode(null, null, buffer) == null)
  }

  test("decode tokens without delimiter") {
    val context = new Context
    import context._

    val buffer = "STORED"
    assert(decoder.decode(null, null, buffer) == null)
  }

  test("decode data") {
    val context = new Context
    import context._

    val buffer = stringToChannelBuffer("VALUE foo 0 1\r\n1\r\nVALUE bar 0 2\r\n12\r\nEND\r\n")
    // These are called once for each state transition (i.e., once per \r\n)
    // by the FramedCodec
    decoder.decode(null, null, buffer)
    decoder.decode(null, null, buffer)
    decoder.decode(null, null, buffer)
    decoder.decode(null, null, buffer)
    assert(decoder.decode(null, null, buffer) == ValueLines(Seq(
      TokensWithData(Seq("VALUE", "foo", "0", "1") map { Buf.Utf8(_) }, Buf.Utf8("1")),
      TokensWithData(Seq("VALUE", "bar", "0", "2") map { Buf.Utf8(_) }, Buf.Utf8("12")))))
  }

  test("decode data with flag") {
    val context = new Context
    import context._

    val buffer = stringToChannelBuffer("VALUE foo 20 1\r\n1\r\nVALUE bar 10 2\r\n12\r\nEND\r\n")
    // These are called once for each state transition (i.e., once per \r\n)
    // by the FramedCodec
    decoder.decode(null, null, buffer)
    decoder.decode(null, null, buffer)
    decoder.decode(null, null, buffer)
    decoder.decode(null, null, buffer)
    assert(decoder.decode(null, null, buffer) == ValueLines(Seq(
      TokensWithData(Seq("VALUE", "foo", "20", "1") map { Buf.Utf8(_) }, Buf.Utf8("1")),
      TokensWithData(Seq("VALUE", "bar", "10", "2") map { Buf.Utf8(_) }, Buf.Utf8("12")))))
  }

  test("decode end") {
    val context = new Context
    import context._

    val buffer = "END\r\n"
    assert(decoder.decode(null, null, buffer) == ValueLines(Seq[TokensWithData]()))
  }

  test("decode stats") {
    val context = new Context
    import context._

    val buffer = stringToChannelBuffer("STAT items:1:number 1\r\nSTAT items:1:age 1468\r\nITEM foo [5 b; 1322514067 s]\r\nEND\r\n")
    decoder.decode(null, null, buffer)
    decoder.decode(null, null, buffer)
    decoder.decode(null, null, buffer)
    val lines = decoder.decode(null, null, buffer)
    assert(lines == StatLines(Seq(
      Tokens(Seq("STAT", "items:1:number", "1") map { Buf.Utf8(_) }),
      Tokens(Seq("STAT", "items:1:age", "1468") map { Buf.Utf8(_) }),
      Tokens(Seq("ITEM", "foo", "[5", "b;", "1322514067", "s]") map { Buf.Utf8(_) })
    )))
  }

}
