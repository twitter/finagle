package com.twitter.finagle.smtp

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.netty3.Netty3Transporter


class SmtpDecoder extends StringDecoder(CharsetUtil.UTF_8) {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): Reply = {
    val rep = super.decode(ctx, channel, msg).asInstanceOf[String]
    val first = rep(0)
    val second = rep(1)
    val third = rep(2)

    if (first.isDigit && second.isDigit && third.isDigit && rep(3).isSpaceChar)
      new Reply {
        val firstDigit = first
        val secondDigit = second
        val thirdDigit = third
        val info = rep.drop(4)
      }
    else
      UnknownReply(rep)
  }
}

object SmtpPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("smtpEncode", new StringEncoder)
    pipeline.addLast("smtpDecode", new SmtpDecoder)
    pipeline.addLast("lf", new DelimEncoder('\n'))
    pipeline
  }
}

class DelimEncoder(delim: Char) extends SimpleChannelHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    val newMessage = evt.getMessage match {
      case m: String => m + delim
      case m => m
    }
    Channels.write(ctx, evt.getFuture, newMessage, evt.getRemoteAddress)
  }
}

object SmtpTransporter extends Netty3Transporter[String, Reply](
  name = "SmtpTransporter",
  pipelineFactory = SmtpPipeline)
/*---*/
