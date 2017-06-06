package com.twitter.finagle.memcached.unit.protocol.text.client

import com.twitter.finagle.memcached.protocol.text.client.ClientDecoder
import com.twitter.finagle.memcached.protocol.text.{Decoding, StatLines, Tokens, TokensWithData, ValueLines}
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DecoderTest extends FunSuite with MockitoSugar {

  class DecodingClientDecoder extends ClientDecoder[Decoding] {
    type Value = TokensWithData

    override protected def parseValue(
      tokens: Seq[Buf],
      data: Buf): TokensWithData = TokensWithData(tokens, data, None)

    protected def parseResponse(tokens: Seq[Buf]): Decoding = Tokens(tokens)
    protected def parseResponseValues(valueLines: Seq[TokensWithData]): Decoding = ValueLines(valueLines)
    protected def parseStatLines(lines: Seq[Tokens]): Decoding = StatLines(lines.map(Tokens(_)))
  }

  test("decode tokens") {
    val decoder = new DecodingClientDecoder

    val buffer = Buf.Utf8("STORED")
    assert(decoder.decode(buffer) == Tokens(Seq(Buf.Utf8("STORED"))))
  }

  test("decode data") {
    val decoder = new DecodingClientDecoder

    decoder.decode(Buf.Utf8("VALUE foo 0 1"))
    decoder.decode(Buf.Utf8("1"))
    decoder.decode(Buf.Utf8("VALUE bar 0 2"))
    decoder.decode(Buf.Utf8("12"))
    assert(decoder.decode(Buf.Utf8("END")) == ValueLines(Seq(
      TokensWithData(Seq("VALUE", "foo", "0", "1") map { Buf.Utf8(_) }, Buf.Utf8("1")),
      TokensWithData(Seq("VALUE", "bar", "0", "2") map { Buf.Utf8(_) }, Buf.Utf8("12")))))
  }

  test("decode data with flag") {
    val decoder = new DecodingClientDecoder

    decoder.decode(Buf.Utf8("VALUE foo 20 1"))
    decoder.decode(Buf.Utf8("1"))
    decoder.decode(Buf.Utf8("VALUE bar 10 2"))
    decoder.decode(Buf.Utf8("12"))
    assert(decoder.decode(Buf.Utf8("END")) == ValueLines(Seq(
      TokensWithData(Seq("VALUE", "foo", "20", "1") map { Buf.Utf8(_) }, Buf.Utf8("1")),
      TokensWithData(Seq("VALUE", "bar", "10", "2") map { Buf.Utf8(_) }, Buf.Utf8("12")))))
  }

  test("decode end") {
    val decoder = new DecodingClientDecoder

    val buffer = Buf.Utf8("END")
    assert(decoder.decode(buffer) == ValueLines(Seq[TokensWithData]()))
  }

  test("decode stats") {
    val decoder = new DecodingClientDecoder

    decoder.decode(Buf.Utf8("STAT items:1:number 1"))
    decoder.decode(Buf.Utf8("STAT items:1:age 1468"))
    decoder.decode(Buf.Utf8("ITEM foo [5 b; 1322514067 s]"))
    val lines = decoder.decode(Buf.Utf8("END"))
    assert(lines == StatLines(Seq(
      Tokens(Seq("STAT", "items:1:number", "1") map { Buf.Utf8(_) }),
      Tokens(Seq("STAT", "items:1:age", "1468") map { Buf.Utf8(_) }),
      Tokens(Seq("ITEM", "foo", "[5", "b;", "1322514067", "s]") map { Buf.Utf8(_) })
    )))
  }

}
