package com.twitter.finagle.stream

import com.twitter.concurrent.ChannelSource
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channels, MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}
import org.jboss.netty.handler.codec.http._

/**
 * Client handler for a streaming protocol.
 */
class HttpChunkToChannel extends SimpleChannelUpstreamHandler {
  private[this] val channelRef =
    new AtomicReference[com.twitter.concurrent.ChannelSource[ChannelBuffer]](null)
  @volatile var numObservers = 0

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage match {
    case message: HttpResponse =>
      require(message.isChunked, "Error: message must be chunked")

      val source = new ChannelSource[ChannelBuffer]
      require(channelRef.compareAndSet(null, source),
        "Channel is already busy, only Chunks are OK at this point.")

      ctx.getChannel.setReadable(false)

      source.numObservers.respond { i =>
        numObservers = i
        i match {
          case 1 =>
            ctx.getChannel.setReadable(true)
          case 0 =>
            // if there are no more observers, then shut everything down
            ctx.getChannel.setReadable(false)
            ctx.getChannel.close()
            source.close()
          case _ =>
        }
        Future.Done
      }

      val response = StreamResponse(message, source)
      Channels.fireMessageReceived(ctx, response)

    case trailer: HttpChunkTrailer =>
      val topic = channelRef.getAndSet(null)
      topic.close()
      ctx.getChannel.setReadable(true)

    case chunk: HttpChunk =>
      ctx.getChannel.setReadable(false)
      val topic = channelRef.get
      Future.join(topic.send(chunk.getContent)) ensure {
        // FIXME serialize on the channel
        if (numObservers > 0) {
          ctx.getChannel.setReadable(true)
        }
      }
  }
}