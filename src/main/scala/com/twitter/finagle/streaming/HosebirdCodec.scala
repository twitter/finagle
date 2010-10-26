package com.twitter.finagle.streaming

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.http.DefaultHttpChunk

class HosebirdCodec extends SimpleChannelUpstreamHandler {
  val parser = new StatusParserJackson
  private val UTF_8 = "UTF-8"

  private def stringFromUtf8Bytes(bytes: Array[Byte]): String =
    new String(bytes, UTF_8)

  override def handleUpstream(ctx: ChannelHandlerContext, c: ChannelEvent) {
    if (!c.isInstanceOf[MessageEvent]) {
      super.handleUpstream(ctx, c)
      return
    }

    val e = c.asInstanceOf[MessageEvent]
    val chunk = e.getMessage().asInstanceOf[DefaultHttpChunk]
    val text = stringFromUtf8Bytes(chunk.getContent().array())
    Channels.fireMessageReceived(ctx,
                                 new CachedMessage(text, parser.parse(text)),
                                 e.getRemoteAddress)
  }
}
