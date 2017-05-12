package com.twitter.finagle.memcached.unit.protocol.text.client

import com.twitter.finagle.memcached.protocol.text.client.ClientDecoder
import com.twitter.finagle.memcached.protocol.text.{TokensWithData, ValueLines, Tokens, StatLines}
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DecoderTest extends FunSuite with MockitoSugar {

  class Context {
    val decoder = new ClientDecoder
  }

  test("decode tokens") {
    val context = new Context
    import context._

    val buffer = Buf.Utf8("STORED")
    assert(decoder.decode(buffer) == Tokens(Seq(Buf.Utf8("STORED"))))
  }

  test("decode data") {
    val context = new Context
    import context._

    decoder.decode(Buf.Utf8("VALUE foo 0 1"))
    decoder.decode(Buf.Utf8("1"))
    decoder.decode(Buf.Utf8("VALUE bar 0 2"))
    decoder.decode(Buf.Utf8("12"))
    assert(decoder.decode(Buf.Utf8("END")) == ValueLines(Seq(
      TokensWithData(Seq("VALUE", "foo", "0", "1") map { Buf.Utf8(_) }, Buf.Utf8("1")),
      TokensWithData(Seq("VALUE", "bar", "0", "2") map { Buf.Utf8(_) }, Buf.Utf8("12")))))
  }

  test("decode data with flag") {
    val context = new Context
    import context._

    decoder.decode(Buf.Utf8("VALUE foo 20 1"))
    decoder.decode(Buf.Utf8("1"))
    decoder.decode(Buf.Utf8("VALUE bar 10 2"))
    decoder.decode(Buf.Utf8("12"))
    assert(decoder.decode(Buf.Utf8("END")) == ValueLines(Seq(
      TokensWithData(Seq("VALUE", "foo", "20", "1") map { Buf.Utf8(_) }, Buf.Utf8("1")),
      TokensWithData(Seq("VALUE", "bar", "10", "2") map { Buf.Utf8(_) }, Buf.Utf8("12")))))
  }

  test("decode end") {
    val context = new Context
    import context._

    val buffer = Buf.Utf8("END")
    assert(decoder.decode(buffer) == ValueLines(Seq[TokensWithData]()))
  }

  test("decode stats") {
    val context = new Context
    import context._

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
