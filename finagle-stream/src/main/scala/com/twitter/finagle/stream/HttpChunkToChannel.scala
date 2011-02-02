package com.twitter.finagle.stream

import org.jboss.netty.handler.codec.http.{HttpChunkTrailer, HttpChunk, HttpMessage}
import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.concurrent.Topic
import org.jboss.netty.channel.{Channels, MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}

class HttpChunkToChannel extends SimpleChannelUpstreamHandler {
  private[this] val topicRef =
    new AtomicReference[com.twitter.concurrent.Topic[ChannelBuffer]](null)

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage match {
    case message: HttpMessage =>
      val topic = new Topic[ChannelBuffer]
      require(topicRef.compareAndSet(null, topic),
        "Channel is already busy")
      ctx.getChannel.setReadable(false)
      topic.onReceive {
        if (!message.isChunked) {
          topic.send(message.getContent)
          topic.close()
          topicRef.set(null)
        }
        ctx.getChannel.setReadable(true)
      }
      Channels.fireMessageReceived(ctx, topic)
    case trailer: HttpChunkTrailer =>
      val topic = topicRef.getAndSet(null)
      topic.close()
    case chunk: HttpChunk =>
      val topic = topicRef.get
      topic.send(chunk.getContent)
  }
}