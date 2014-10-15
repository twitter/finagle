package com.twitter.finagle.smtp.transport

import com.twitter.finagle.smtp.reply._
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.util.CharsetUtil

/**
 * Decodes SMTP replies from lines ending with <CRLF>.
 * An SMTP reply is a three-digit code followed by
 * space and an optional informational string.
 *
 * If the three-digit code is followed by a hyphen,
 * treats the line as a part of a multiline reply.
 */
class SmtpDecoder extends LineBasedFrameDecoder(1000) {
  import CodecUtil._
  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: ChannelBuffer): UnspecifiedReply = {
    Option(super.decode(ctx, channel, msg)) match {
      case None => null
      case Some(buf: ChannelBuffer) =>
        val rep = buf.toString(CharsetUtil.US_ASCII)
        if (rep.length < 4)
          InvalidReply(rep)
        else {
          val first::second::third::delim::info = rep.toList

          // Standard reply: three-digit-code SP info
          if (first.isDigit && second.isDigit && third.isDigit)
            delim match {
              case ' ' =>
                new UnspecifiedReply {
                  val code = getCode(rep)
                  val info = getInfo(rep)
                }

              case '-' =>
                val pipeline = ctx.getPipeline
                val code = getCode(rep)
                NonTerminalLine(code, getInfo(rep))


              case _ => InvalidReply(rep)
            }

          else
            InvalidReply(rep)
        }

    }
  }
}
