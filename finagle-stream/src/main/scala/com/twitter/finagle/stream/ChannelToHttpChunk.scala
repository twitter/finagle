package com.twitter.finagle.stream

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.channel._
import java.util.concurrent.atomic.AtomicReference
import com.twitter.concurrent.Observer

/**
 * A Netty Channel Handler that adapts Twitter Channels to Netty Channels.
 * Note that a Twitter Channel is unlike a Netty Channel despite having
 * the same name. A Twitter Channel is a uniplex, asynchronous intra-process
 * communication channel, whereas a Netty Channel typically represents
 * a duplex, socket-based comunication channel.
 */
class ChannelToHttpChunk extends SimpleChannelDownstreamHandler {
  private[this] val channelRef =
    new AtomicReference[com.twitter.concurrent.Channel[ChannelBuffer]](null)
  private[this] val observerRef =
    new AtomicReference[Observer](null)

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage match {
    case channel: com.twitter.concurrent.Channel[ChannelBuffer] =>
      require(channelRef.compareAndSet(null, channel), "Channel is already busy.")

      val startMessage = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      HttpHeaders.setHeader(startMessage, "Transfer-Encoding", "Chunked")

      val ignoreOutcome = new DefaultChannelFuture(ctx.getChannel, false)
      Channels.write(ctx, ignoreOutcome, startMessage)
      val observer = channel.respond(this) { channelBuffer =>
        val ignoreOutcome = new DefaultChannelFuture(ctx.getChannel, false)
        Channels.write(ctx, ignoreOutcome, new DefaultHttpChunk(channelBuffer))
      }
      observerRef.set(observer)
      channel.closes.respond { _ =>
        val ignoreOutcome = new DefaultChannelFuture(ctx.getChannel, false)
        channelRef.set(null)
        observerRef.set(null)
        Channels.write(ctx, ignoreOutcome, new DefaultHttpChunkTrailer)
      }
    case _ =>
      throw new IllegalArgumentException("Expecting a Channel" + e.getMessage)
  }


  override def setInterestOpsRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) = {
    val observer = observerRef.get
    if (ctx.getChannel.isWritable) observer.resume()
    else observer.pause()
  }

  override def closeRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val channel = channelRef.get
    if (channel ne null) channel.close()
  }
}