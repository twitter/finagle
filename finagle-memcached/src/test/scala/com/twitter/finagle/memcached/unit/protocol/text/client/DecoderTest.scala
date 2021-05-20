package com.twitter.finagle.memcached.unit.protocol.text.client

import com.twitter.finagle.memcached.protocol.text.client.ClientDecoder
import com.twitter.finagle.memcached.protocol.text.{
  Decoding,
  StatLines,
  Tokens,
  TokensWithData,
  ValueLines
}
import com.twitter.io.Buf
import org.scalatestplus.mockito.MockitoSugar
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class DecoderTest extends AnyFunSuite with MockitoSugar {

  private class DecodingClientDecoder extends ClientDecoder[Decoding] {
    type Value = TokensWithData

    protected def parseValue(tokens: Seq[Buf], data: Buf): TokensWithData =
      TokensWithData(tokens, data, None)

    protected def parseResponse(tokens: Seq[Buf]): Decoding = Tokens(tokens)
    protected def parseResponseValues(valueLines: Seq[TokensWithData]): Decoding =
      ValueLines(valueLines)
    protected def parseStatLines(lines: Seq[Tokens]): Decoding = StatLines(lines.map(Tokens(_)))
  }

  private class Context {
    val decoder = new DecodingClientDecoder

    def decodeString(data: String): Seq[Decoding] = {
      val out = new mutable.ArrayBuffer[Decoding]()
      decoder.decodeData(Buf.Utf8(data), out)
      out.toSeq
    }
  }

  test("decode tokens") {
    val context = new Context
    assert(context.decodeString("STORED") == Seq(Tokens(Seq(Buf.Utf8("STORED")))))
  }

  test("decode data") {
    val context = new Context

    assert(context.decodeString("VALUE foo 0 1").isEmpty)
    assert(context.decodeString("1").isEmpty)
    assert(context.decodeString("VALUE bar 0 2").isEmpty)
    assert(context.decodeString("12").isEmpty)
    assert(
      context.decodeString("END") == Seq(
        ValueLines(
          Seq(
            TokensWithData(Seq("VALUE", "foo", "0", "1") map { Buf.Utf8(_) }, Buf.Utf8("1")),
            TokensWithData(Seq("VALUE", "bar", "0", "2") map { Buf.Utf8(_) }, Buf.Utf8("12"))
          )
        )
      )
    )
  }

  test("decode data with flag") {
    val context = new Context

    assert(context.decodeString("VALUE foo 20 1").isEmpty)
    assert(context.decodeString("1").isEmpty)
    assert(context.decodeString("VALUE bar 10 2").isEmpty)
    assert(context.decodeString("12").isEmpty)
    assert(
      context.decodeString("END") == Seq(
        ValueLines(
          Seq(
            TokensWithData(Seq("VALUE", "foo", "20", "1") map { Buf.Utf8(_) }, Buf.Utf8("1")),
            TokensWithData(Seq("VALUE", "bar", "10", "2") map { Buf.Utf8(_) }, Buf.Utf8("12"))
          )
        )
      )
    )
  }

  test("decode end") {
    val context = new Context

    assert(context.decodeString("END") == Seq(ValueLines(Seq[TokensWithData]())))
  }

  test("decode stats") {
    val context = new Context

    assert(context.decodeString("STAT items:1:number 1").isEmpty)
    assert(context.decodeString("STAT items:1:age 1468").isEmpty)
    assert(context.decodeString("ITEM foo [5 b; 1322514067 s]").isEmpty)
    val lines = context.decodeString("END")
    assert(
      lines == Seq(
        StatLines(
          Seq(
            Tokens(Seq("STAT", "items:1:number", "1") map { Buf.Utf8(_) }),
            Tokens(Seq("STAT", "items:1:age", "1468") map { Buf.Utf8(_) }),
            Tokens(Seq("ITEM", "foo", "[5", "b;", "1322514067", "s]") map { Buf.Utf8(_) })
          )
        )
      )
    )
  }

}
