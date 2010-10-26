package com.twitter.finagle.streaming

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.http.HttpChunk

class StreamingCodec extends SimpleChannelUpstreamHandler {
  val parser = new StatusParserJackson
  private val UTF_8 = "UTF-8"

  private def stringFromUtf8Bytes(bytes: Array[Byte]): String =
    new String(bytes, UTF_8)

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case chunk: HttpChunk =>
        val text = stringFromUtf8Bytes(chunk.getContent().array())
        Channels.fireMessageReceived(
          ctx, new CachedMessage(text, parser.parse(text)),
          e.getRemoteAddress)
      case _ => ()                      // swallow
    }

  }
}
