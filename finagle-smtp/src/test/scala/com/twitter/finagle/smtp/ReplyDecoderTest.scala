package com.twitter.finagle.smtp

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.smtp.transport.{CodecUtil, ReplyDecoder}
import com.twitter.finagle.smtp.reply.{InvalidReply, MultilinePart, UnspecifiedReply}
import org.specs.mock.MockitoStubs

@RunWith(classOf[JUnitRunner])
class ReplyDecoderTest extends FunSuite with MockitoStubs {
  val ctx = mock[ChannelHandlerContext]
  val channel = mock[Channel]

  test("decode standart correct reply") {
    val pipeline = Channels.pipeline()
    ctx.getPipeline returns pipeline

    val rep = "250 OK\r\n"
    val msg = ChannelBuffers.copiedBuffer(rep, CharsetUtil.US_ASCII)
    val decoder = new ReplyDecoder
    val decoded = decoder.decode(ctx, channel, msg)
    assert(decoded.code === 250)
    assert(decoded.info === "OK")
    assert(decoded.isMultiline === false)
  }

  test("decode multiline reply") {
    val rep = Seq("250-answer to ehlo\r\n", "250 extension and end\r\n")

    val testSmtpDecode = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
        assert(ctx.getPipeline.get(CodecUtil.aggregation) != null)
        val msg = e.getMessage
        //asserts for the final message
        assert(msg.isInstanceOf[UnspecifiedReply])
        assert(msg.asInstanceOf[UnspecifiedReply].code === 250)
        assert(msg.asInstanceOf[UnspecifiedReply].info === "answer to ehlo")
        assert(msg.asInstanceOf[UnspecifiedReply].isMultiline === true)
        assert(msg.asInstanceOf[UnspecifiedReply].lines === Seq("answer to ehlo", "extension and end"))
      }
    }
    val pipeline = Channels.pipeline()
    pipeline.addLast("smtpDecode", testSmtpDecode)
    ctx.getPipeline returns pipeline

    val msg = rep map {ChannelBuffers.copiedBuffer(_, CharsetUtil.US_ASCII)}
    val decoder = new ReplyDecoder

    //asserts for non-terminal lines
    val decoded1 = decoder.decode(ctx, channel, msg(0))
    assert(decoded1.isInstanceOf[MultilinePart])
    assert(decoded1.code === 250)
    assert(decoded1.info === "answer to ehlo")

    //asserts for the terminal line
    val decoded2 = decoder.decode(ctx, channel, msg(1))
    assert(!decoded2.isInstanceOf[MultilinePart])
    assert(decoded2.code === 250)
    assert(decoded2.info === "extension and end")
  }

  test("wrap invalid replies") {
    val pipeline = Channels.pipeline()
    ctx.getPipeline returns pipeline

    val rep = "250smth wrong\r\n"
    val msg = ChannelBuffers.copiedBuffer(rep, CharsetUtil.US_ASCII)
    val decoder = new ReplyDecoder
    val decoded = decoder.decode(ctx, channel, msg)
    assert(decoded.isInstanceOf[InvalidReply])
  }
}
