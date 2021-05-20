package com.twitter.finagle.netty4.decoder

import com.twitter.finagle.Failure
import com.twitter.finagle.decoder.Framer
import com.twitter.io.Buf
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.scalatestplus.mockito.MockitoSugar
import scala.collection.mutable.ArrayBuffer
import org.scalatest.funsuite.AnyFunSuite

class DecoderHandlerTest extends AnyFunSuite with MockitoSugar {

  object FixedFramer extends TestFramer(4)

  def toBuf(s: String): Buf = Buf.Utf8(s)
  def toStr(b: Buf): String = Buf.Utf8.unapply(b).get

  test("DecoderHandler handles frames") {
    val messagesSeen = new ArrayBuffer[String]
    var nonStringsMessageCount = 0
    val readSnooper = new ChannelInboundHandlerAdapter {
      override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
        msg match {
          case b: Buf =>
            messagesSeen.append(toStr(b))
          case _ =>
            nonStringsMessageCount += 1
        }
        super.channelRead(ctx, msg)
      }
    }
    val ch = new EmbeddedChannel(new DecoderHandler(FixedFramer), readSnooper)
    ch.pipeline.fireChannelActive

    ch.writeInbound(toBuf("hi"))
    assert(nonStringsMessageCount == 0)
    assert(messagesSeen.isEmpty)

    ch.writeInbound(toBuf("2u"))
    assert(nonStringsMessageCount == 0)
    assert(messagesSeen.head == "hi2u")

    ch.writeInbound(toBuf("a big string"))
    assert(nonStringsMessageCount == 0)
    assert(messagesSeen.drop(1) == "a big string".grouped(4).toList)
  }

  test("DecoderHandler doesn't swallow exceptions thrown by decoder") {
    val exnThrown = new Exception("boom")
    val failingFramer = new Framer {
      def apply(buf: Buf): IndexedSeq[Buf] = throw exnThrown
    }

    val ch = new EmbeddedChannel(new DecoderHandler(failingFramer))
    ch.pipeline.fireChannelActive
    val exnSeen = intercept[Exception] { ch.writeInbound(Buf.Empty) }
    assert(exnThrown == exnSeen)
  }

  test("DecoderHandler throws exceptions when handling non-Buf messages") {
    val notABuf = "I'm definitely not a buf"
    val noopFramer = new Framer {
      def apply(b: Buf) = { IndexedSeq.empty[Buf] }
    }

    val ch = new EmbeddedChannel(new DecoderHandler(noopFramer))
    ch.pipeline.fireChannelActive
    intercept[Failure] { ch.writeInbound("Not a buf") }
  }
}
