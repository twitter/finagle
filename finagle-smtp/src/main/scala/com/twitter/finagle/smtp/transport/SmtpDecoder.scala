package com.twitter.finagle.smtp.transport

import org.jboss.netty.channel.{Channels, MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}
import com.twitter.finagle.smtp.reply._

/**
 * Removes from pipeline, if found, [[com.twitter.finagle.smtp.transport.AggregateMultiline]] handler
 * that may have been temporarily added there.
 * If the received message is not an [[com.twitter.finagle.smtp.reply.UnspecifiedReply]],
 * wraps it into an [[com.twitter.finagle.smtp.reply.InvalidReply]]*/
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
