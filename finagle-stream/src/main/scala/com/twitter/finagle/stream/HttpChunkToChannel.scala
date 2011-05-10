package com.twitter.finagle.stream

import com.twitter.concurrent.ChannelSource
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channels, ChannelHandlerContext, ChannelStateEvent,
  ExceptionEvent, MessageEvent, SimpleChannelUpstreamHandler}
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
      Console.println("HttpChunkToChannel.messageReceived: message=" + message)
      val source = new ChannelSource[ChannelBuffer]
      require(channelRef.compareAndSet(null, source),
        "Channel is already busy, only Chunks are OK at this point.")

      val response = new StreamResponse {
        val httpResponse = message
        val channel = source
        def release() {
          ctx.getChannel.close()
          source.close()
        }
      }

      ctx.getChannel.setReadable(false)

      source.numObservers.respond { i =>
        numObservers = i
        Console.println("numObservers=" + i)
        i match {
          case 1 =>
            if (!message.isChunked) {
              val content = message.getContent
              if (content.readable) {
                Future.join(source.send(content))
              }
            }
            ctx.getChannel.setReadable(true)
          case 0 =>
            // if there are no more observers, then shut everything down
            response.release()
          case _ =>
        }
        Future.Done
      }

      Channels.fireMessageReceived(ctx, response)

    case chunk: HttpChunk if chunk.isLast  =>
      Console.println("HttpChunkToChannel.messageReceived: trailer=" + chunk)
      val topic = channelRef.getAndSet(null)
      Future.join(topic.send(chunk.getContent))
      topic.close()
      ctx.getChannel.setReadable(true)

    case chunk: HttpChunk =>
      Console.println("HttpChunkToChannel.messageReceived: chunk=" + chunk)
      ctx.getChannel.setReadable(false)
      val topic = channelRef.get
      Future.join(topic.send(chunk.getContent)) ensure {
        // FIXME serialize on the channel
        if (numObservers > 0) {
          ctx.getChannel.setReadable(true)
        }
      }
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    Option(channelRef.get).foreach(_.close())
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    Option(channelRef.get).foreach(_.close())
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent)  {
    Console.println(e)
    e.getCause.printStackTrace
    // should we pass the exception to the observer somehow?
  }
}
