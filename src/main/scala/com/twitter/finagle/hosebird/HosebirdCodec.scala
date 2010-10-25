package com.twitter.finagle.hosebird

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}


class HosebirdCodec extends SimpleChannelUpstreamHandler {
  val parser = new StatusParserJackson

  override def handleUpstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleUpstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]
    val state = parser.parseForState(e.getMessage.toString).wrapped

    println("State: %s".format(state))
    Channels.fireMessageReceived(ctx, state, e.getRemoteAddress)
  }
}

class Chatty extends SimpleChannelUpstreamHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e match {
      case status: Status => println("%s: %s".format(status.state.userIdOpt.get, status.state.textOpt.get))
      case _ => println("Other shit")
    }
  }
}
