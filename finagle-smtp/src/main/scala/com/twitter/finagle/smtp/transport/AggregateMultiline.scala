package com.twitter.finagle.smtp.transport

import org.jboss.netty.channel.{Channels, MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}
import com.twitter.finagle.smtp.reply._

/*Aggregates replies in one multiline reply*/
case class AggregateMultiline(multiline_code: Int, lns: Seq[String]) extends SimpleChannelUpstreamHandler {
  import CodecUtil._
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case NonTerminalLine(code, next) =>
        if (code == multiline_code){
          val pipeline = ctx.getPipeline
          pipeline.replace(aggregation, aggregation, copy(lns = lns :+ next))
        }
        else {
          val invalid = new InvalidReply(lns.head) {
            override val code = multiline_code
            override val isMultiline = true
            override val lines = lns :+ next
          }
          Channels.fireMessageReceived(ctx, invalid)
        }

      case InvalidReply(next) => {
        val invalid = new InvalidReply(lns.head) {
          override val code = multiline_code
          override val isMultiline = true
          override val lines = lns :+ next
        }
        Channels.fireMessageReceived(ctx, invalid)
      }
      //last element in the list
      case last: UnspecifiedReply => {
        val multiline = new UnspecifiedReply {
          val info: String = lns.head
          val code: Int = multiline_code
          override val lines = lns :+ last.info
          override val isMultiline = true
        }
        Channels.fireMessageReceived(ctx, multiline)
      }
      case _ => ctx.sendUpstream(e)
    }
  }
}
