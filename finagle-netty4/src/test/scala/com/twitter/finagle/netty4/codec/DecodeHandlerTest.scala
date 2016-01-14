package com.twitter.finagle.netty4.codec

import com.twitter.finagle.codec.{FrameDecoder, FixedLengthDecoder}
import com.twitter.io.Buf
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class DecodeHandlerTest extends FunSuite with MockitoSugar {

  object StringDecoder extends FixedLengthDecoder(4, Buf.Utf8.unapply(_).getOrElse("????"))

  test("DecodeHandler handles decodes") {
    val messagesSeen = new ArrayBuffer[String]
    var nonStringsMessageCount = 0
    val readSnooper = new ChannelInboundHandlerAdapter {
      override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
        msg match {
          case s: String =>
            messagesSeen.append(s)
          case _ =>
            nonStringsMessageCount += 1
        }
        super.channelRead(ctx, msg)
      }
    }
    val ch = new EmbeddedChannel(new DecodeHandler[String](() => StringDecoder), readSnooper)
    ch.pipeline.fireChannelActive

    ch.writeInbound(Unpooled.wrappedBuffer("hi".getBytes))
    assert(nonStringsMessageCount == 0)
    assert(messagesSeen.isEmpty)

    ch.writeInbound(Unpooled.wrappedBuffer("2u".getBytes))
    assert(nonStringsMessageCount == 0)
    assert(messagesSeen.head == "hi2u")

    ch.writeInbound(Unpooled.wrappedBuffer("a big string".getBytes))
    assert(nonStringsMessageCount == 0)
    assert(messagesSeen.drop(1) == "a big string".grouped(4).toList)
  }

  test("DecoderHandler doesn't swallow exceptions thrown by decoder") {
    val exnThrown = new Exception("boom")
    val failingDecoder = new FrameDecoder[String] {
      def apply(buf: Buf): IndexedSeq[String] = throw exnThrown
    }

    val ch = new EmbeddedChannel(new DecodeHandler[String](() => failingDecoder))
    ch.pipeline.fireChannelActive
    val exnSeen = intercept[Exception] { ch.writeInbound(Unpooled.EMPTY_BUFFER) }
    assert(exnThrown == exnSeen)
  }
}
