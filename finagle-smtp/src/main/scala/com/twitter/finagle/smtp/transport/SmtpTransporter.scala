package com.twitter.finagle.smtp.transport

import org.jboss.netty.channel._
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.util.NonFatal
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder
import com.twitter.finagle.smtp.Request
import com.twitter.finagle.smtp.reply.UnspecifiedReply

object SmtpPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    //pipeline.addLast("logger", new LoggingHandler(InternalLogLevel.INFO))
    pipeline.addLast("smtpEncode", new SmtpEncoder)
    pipeline.addLast("replyDecode", new ReplyDecoder)
    pipeline.addLast("smtpDecode", new SmtpDecoder)
    pipeline
  }
}

object SmtpTransporter extends Netty3Transporter[Request, UnspecifiedReply](
  name = "SmtpTransporter",
  pipelineFactory = SmtpPipeline)

