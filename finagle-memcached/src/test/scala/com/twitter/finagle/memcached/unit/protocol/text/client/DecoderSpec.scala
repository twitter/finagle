package com.twitter.finagle.memcached.unit.protocol.text.client

import org.specs.Specification
import com.twitter.finagle.memcached.protocol.text.client.Decoder
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.protocol.text.{TokensWithData, ValueLines, Tokens}
import org.specs.mock.Mockito

object DecoderSpec extends Specification with Mockito {
  "Decoder" should {
    val decoder = new Decoder
    decoder.start()

    "decode" in {
      "tokens" in {
        "with full delimiter" in {
          val buffer = "STORED\r\n"
          decoder.decode(null, null, buffer) mustEqual Tokens(Seq("STORED"))
        }

        "with partial delimiter" in {
          val buffer = "STORED\r"
          decoder.decode(null, null, buffer) mustBe null
        }

        "without delimiter" in {
          val buffer = "STORED"
          decoder.decode(null, null, buffer) mustBe null
        }
      }

      "data" in {
        val buffer = stringToChannelBuffer("VALUE foo 0 1\r\n1\r\nVALUE bar 0 2\r\n12\r\nEND\r\n")
        // These are called once for each state transition (i.e., once per \r\n)
        // by the FramedCodec
        decoder.decode(null, null, buffer)
        decoder.decode(null, null, buffer)
        decoder.decode(null, null, buffer)
        decoder.decode(null, null, buffer)
        decoder.decode(null, null, buffer) mustEqual ValueLines(Seq(
          TokensWithData(Seq("VALUE", "foo", "0", "1"), "1"),
          TokensWithData(Seq("VALUE", "bar", "0", "2"), "12")))
      }

      "data with flag" in {
        val buffer = stringToChannelBuffer("VALUE foo 20 1\r\n1\r\nVALUE bar 10 2\r\n12\r\nEND\r\n")
        // These are called once for each state transition (i.e., once per \r\n)
        // by the FramedCodec
        decoder.decode(null, null, buffer)
        decoder.decode(null, null, buffer)
        decoder.decode(null, null, buffer)
        decoder.decode(null, null, buffer)
        decoder.decode(null, null, buffer) mustEqual ValueLines(Seq(
          TokensWithData(Seq("VALUE", "foo", "20", "1"), "1"),
          TokensWithData(Seq("VALUE", "bar", "10", "2"), "12")))
      }

      "end" in {
        val buffer = "END\r\n"
        decoder.decode(null, null, buffer) mustEqual ValueLines(Seq[TokensWithData]())
      }
    }
  }
}
