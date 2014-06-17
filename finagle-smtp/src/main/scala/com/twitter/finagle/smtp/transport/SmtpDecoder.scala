package com.twitter.finagle.smtp.transport

import org.jboss.netty.channel.{Channels, MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}
import com.twitter.finagle.smtp.reply._

class SmtpDecoder extends SimpleChannelUpstreamHandler{
  import CodecUtil._
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val pipeline = ctx.getPipeline
    if (pipeline.get(aggregation) != null)
      pipeline.remove(aggregation)
    e.getMessage match {
      case rep: UnspecifiedReply => Channels.fireMessageReceived(ctx, rep)
      case other => Channels.fireMessageReceived(ctx, InvalidReply(other.toString))
    }
  }
}
