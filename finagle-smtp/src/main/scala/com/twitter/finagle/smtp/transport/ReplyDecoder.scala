package com.twitter.finagle.smtp.transport

import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.smtp.reply._

class ReplyDecoder extends LineBasedFrameDecoder(100) {
  import CodecUtil._
  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: ChannelBuffer): UnspecifiedReply = {
    super.decode(ctx, channel, msg) match {
      case cb: ChannelBuffer => {
        val rep = cb.toString(CharsetUtil.UTF_8)
        val first = rep(0)
        val second = rep(1)
        val third = rep(2)

        //Standart reply: three-digit-code SP info
        if (first.isDigit && second.isDigit && third.isDigit)
          rep(3) match {
            case ' ' =>
              new UnspecifiedReply {
                val code = getCode(rep)
                val info = getInfo(rep)
              }

            case '-' =>
              val pipeline = ctx.getPipeline
              val code = getCode(rep)
              if (pipeline.get(aggregation) == null)
                pipeline.addBefore("smtpDecode", aggregation, AggregateMultiline(code, Seq[String]()))
              MultilinePart(code, getInfo(rep))


            case _ => InvalidReply(rep)
          }

        else
          InvalidReply(rep)
      }
    }
  }
}
