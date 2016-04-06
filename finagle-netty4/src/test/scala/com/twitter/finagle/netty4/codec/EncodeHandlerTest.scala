package com.twitter.finagle.netty4.codec

import com.twitter.finagle.netty4.ByteBufAsBuf
import com.twitter.finagle.Failure
import com.twitter.finagle.codec.FrameEncoder
import com.twitter.io.Buf
import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EncodeHandlerTest extends FunSuite with GeneratorDrivenPropertyChecks {
  test("encodes)") {
    val encode: FrameEncoder[String] = new FrameEncoder[String] {
      def apply(s: String): Buf = Buf.Utf8(s)
    }

    val ch = new EmbeddedChannel(new EncodeHandler[String](encode))
    ch.pipeline.fireChannelActive

    forAll { s: String =>
      val written = ch.pipeline.writeAndFlush(s)
      assert(written.await().isDone)
      assert(s == "" || ByteBufAsBuf.Owned(ch.readOutbound[ByteBuf]) == Buf.Utf8(s))
    }
  }

  test("fails a write promise on encoding failure") {
    val exnThrown = new RuntimeException("worst. string. ever.")
    val encode = new FrameEncoder[String] { def apply(s: String): Buf = throw exnThrown }

    val ch = new EmbeddedChannel(new EncodeHandler[String](encode))
    ch.pipeline.fireChannelActive

    val p = ch.pipeline.writeAndFlush("doomed")
    assert(p.await().cause().asInstanceOf[Failure].cause == Some(exnThrown))
  }
}
