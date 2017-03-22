package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar

@RunWith(classOf[JUnitRunner])
class EncoderTest extends FunSuite with MockitoSugar {

  test("not alter the tokens it is serializing") {
    val encoder = new Encoder

    def encode(decoding: Decoding) = {
      val encoded = encoder.encode(decoding)
      Buf.Utf8.unapply(encoded).get
    }

    def encodeIsPure(decoding: Decoding) = {
      val buf1 = encode(decoding)
      val buf2 = encode(decoding)
      assert(buf1 == buf2)
    }

    info("tokens")
    encodeIsPure(Tokens(Seq(Buf.Utf8("tok"))))

    info("tokens with data")
    encodeIsPure(TokensWithData(Seq(Buf.Utf8("foo")), Buf.Utf8("bar"), None))

    info("tokens with data and cas")
    encodeIsPure(TokensWithData(Seq(Buf.Utf8("foo")), Buf.Utf8("baz"), Some(Buf.Utf8("quux"))))

    info("stat lines")
    encodeIsPure(
      StatLines(
        Seq(
          Tokens(Seq(Buf.Utf8("tok1"))),
          Tokens(Seq(Buf.Utf8("tok2")))
        )
      )
    )

    info("value lines")
    encodeIsPure(
      ValueLines(Seq(TokensWithData(Seq(Buf.Utf8("foo")), Buf.Utf8("bar"), Some(Buf.Utf8("quux")))))
    )
  }
}

