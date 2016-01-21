package com.twitter.finagle.netty4.codec

import collection.mutable.ArrayBuffer
import com.twitter.finagle.codec.FrameEncoder
import com.twitter.io.Buf
import com.twitter.io.Charsets.Utf8
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import java.nio.charset.Charset
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class EncodeHandlerTest extends FunSuite with MockitoSugar {
  test("EncodeHandler handles encodes") {
    val messagesSeen = new ArrayBuffer[ByteBuf]
    val writeSnooper = new ChannelOutboundHandlerAdapter {

      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
        msg match {
          case s: ByteBuf =>
            messagesSeen.append(s)
          case _ =>
            fail("expected ByteBuf message")
        }
        super.write(ctx, msg, promise)
      }
    }
    val encode: FrameEncoder[String] = new FrameEncoder[String] {
      def apply(s: String): Buf = Buf.Utf8(s)
    }

    val ch = new EmbeddedChannel(writeSnooper, new EncodeHandler[String](encode))
    ch.pipeline.fireChannelActive

    ch.pipeline.writeAndFlush("hello")

    assert(messagesSeen(0).toString(Utf8) == "hello")

    ch.pipeline.writeAndFlush("a big string")
    assert(messagesSeen(1).toString(Utf8) == "a big string")
  }

  test("EncodeHandler fires an exception event on encoding failure") {
    @volatile var exnSeen: Throwable = null
    val exnSnooper = new ChannelInboundHandlerAdapter {
      override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
        exnSeen = cause.getCause
        super.exceptionCaught(ctx, cause)
      }
    }

    val exnThrown = new RuntimeException("worst. string. ever.")
    val encode = new FrameEncoder[String] { def apply(s: String): Buf = throw exnThrown }

    val ch = new EmbeddedChannel(exnSnooper, new EncodeHandler[String](encode))
    ch.pipeline.fireChannelActive
    ch.pipeline.writeAndFlush("doomed")
    assert(exnSeen == exnThrown)
  }
}
