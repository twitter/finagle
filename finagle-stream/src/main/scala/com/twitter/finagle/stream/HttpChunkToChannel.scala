package com.twitter.finagle.stream

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channels, MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import com.twitter.concurrent.{Serialized, ChannelSource}
import org.jboss.netty.handler.codec.http._

/**
 * Client handler for a streaming protocol.
 */
class HttpChunkToChannel extends SimpleChannelUpstreamHandler with Serialized {
  private[this] val channelRef =
    new AtomicReference[com.twitter.concurrent.ChannelSource[ChannelBuffer]](null)

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage match {
    case message: HttpResponse =>
      require(message.getStatus == HttpResponseStatus.OK,
        "Error: " + message.getStatus)
      val source = new ChannelSource[ChannelBuffer]
      require(channelRef.compareAndSet(null, source),
        "Channel is already busy")

      ctx.getChannel.setReadable(false)
      source.responds.first.respond { _ =>
        if (!message.isChunked) {
          source.send(message.getContent)
          source.close()
          channelRef.set(null)
        }
        ctx.getChannel.setReadable(true)
      }
      var pausedCount = 0
      source.pauses.respond(this) { _ =>
        serialized {
          pausedCount += 1
          if (pausedCount == 1) ctx.getChannel.setReadable(false)
        }
      }
      source.resumes.respond(this) { _ =>
        serialized {
          pausedCount -= 1
          if (pausedCount == 0) ctx.getChannel.setReadable(true)
        }
      }
      Channels.fireMessageReceived(ctx, source)
    case trailer: HttpChunkTrailer =>
      val topic = channelRef.getAndSet(null)
      topic.close()
    case chunk: HttpChunk =>
      val topic = channelRef.get
      topic.send(chunk.getContent)
  }
}