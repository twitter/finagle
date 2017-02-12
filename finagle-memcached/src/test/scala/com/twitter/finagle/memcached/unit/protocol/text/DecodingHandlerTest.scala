package com.twitter.finagle.memcached.unit.protocol.text

import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.ServerError
import com.twitter.finagle.memcached.protocol.text.client.ClientDecoder
import com.twitter.finagle.memcached.protocol.text.{Decoding, DecodingHandler, Tokens}
import com.twitter.io.Buf
import com.twitter.util.{Await, Promise}
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DecodingHandlerTest extends FunSuite {

  test("Decodes Bufs into Decodings") {
    new CodecTestHelper {
      val decodingHandler: DecodingHandler = new DecodingHandler(new ClientDecoder)

      val buf: Buf = Buf.Utf8("STORED")
      val shouldDecodeTo: Decoding = Tokens(Seq(Buf.Utf8("STORED")))

      when(msgEvent.getMessage).thenReturn(buf, Nil: _*)
      decodingHandler.messageReceived(context, msgEvent)

      val result = Await.result(received, 2.seconds)
      assert(result.isInstanceOf[Decoding])
      assert(result.asInstanceOf[Decoding] == shouldDecodeTo)
    }
  }

  test("Resets decoder on exceptions") {
    new CodecTestHelper {
      val decodingHandler: DecodingHandler = new DecodingHandler(new ClientDecoder)

      when(msgEvent.getMessage).thenReturn(Buf.Utf8("STAT"), Nil: _*)
      decodingHandler.messageReceived(context, msgEvent)

      written = new Promise[scala.Any]
      received = new Promise[scala.Any]

      // Decoder has now transitioned to awaiting stats, so GARBAGE causes an error
      when(msgEvent.getMessage).thenReturn(Buf.Utf8("GARBAGE"), Nil: _*)

      intercept[ServerError] {
        decodingHandler.messageReceived(context, msgEvent)
        Await.result(received, 2.seconds)
      }

      // State should be reset, GARBAGE should be valid response
      written = new Promise[scala.Any]
      received = new Promise[scala.Any]

      when(msgEvent.getMessage).thenReturn(Buf.Utf8("GARBAGE"), Nil: _*)
      decodingHandler.messageReceived(context, msgEvent)

      val result = Await.result(received, 2.seconds)
      assert(result.isInstanceOf[Decoding])
      assert(result.asInstanceOf[Decoding] == Tokens(Seq(Buf.Utf8("GARBAGE"))))
    }
  }
}
