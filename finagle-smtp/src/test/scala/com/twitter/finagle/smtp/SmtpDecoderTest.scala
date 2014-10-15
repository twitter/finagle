package com.twitter.finagle.smtp

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.util.CharsetUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import com.twitter.finagle.smtp.transport.{CodecUtil, SmtpDecoder}
import com.twitter.finagle.smtp.reply.{InvalidReply, NonTerminalLine, UnspecifiedReply}

@RunWith(classOf[JUnitRunner])
class SmtpDecoderTest extends FunSuite with MockitoSugar {
  val ctx = mock[ChannelHandlerContext]
  val channel = mock[Channel]

  test("decode standart correct reply") {
    val rep = "250 OK\r\n"
    val msg = ChannelBuffers.copiedBuffer(rep, CharsetUtil.US_ASCII)
    val decoder = new SmtpDecoder
    val decoded = decoder.decode(ctx, channel, msg)
    assert(decoded.code === 250)
    assert(decoded.info === "OK")
    assert(decoded.isMultiline === false)
  }

  test("decode multiline reply part") {
    val rep = Seq("250-answer to ehlo\r\n", "250 extension and end\r\n")

    val msg = rep map {ChannelBuffers.copiedBuffer(_, CharsetUtil.US_ASCII)}
    val decoder = new SmtpDecoder

    //asserts for non-terminal lines
    val decoded1 = decoder.decode(ctx, channel, msg(0))
    assert(decoded1.isInstanceOf[NonTerminalLine])
    assert(decoded1.code === 250)
    assert(decoded1.info === "answer to ehlo")

    //asserts for the terminal line
    val decoded2 = decoder.decode(ctx, channel, msg(1))
    assert(!decoded2.isInstanceOf[NonTerminalLine])
    assert(decoded2.code === 250)
    assert(decoded2.info === "extension and end")
  }

  test("wrap invalid replies") {
    val rep = "250smth wrong\r\n"
    val msg = ChannelBuffers.copiedBuffer(rep, CharsetUtil.US_ASCII)
    val decoder = new SmtpDecoder
    val decoded = decoder.decode(ctx, channel, msg)
    assert(decoded.isInstanceOf[InvalidReply])
  }
}
