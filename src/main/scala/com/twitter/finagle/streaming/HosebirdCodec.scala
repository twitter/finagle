package com.twitter.finagle.streaming

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.http.DefaultHttpChunk

class HosebirdCodec extends SimpleChannelUpstreamHandler {
  val parser = new StatusParserJackson

  override def handleUpstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleUpstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]
    val text = new String(e.getMessage().asInstanceOf[DefaultHttpChunk].getContent().array(), "UTF-8")
    Channels.fireMessageReceived(ctx,
                                 new CachedMessage(text, parser.parse(text)),
                                 e.getRemoteAddress)
  }
}
