package com.twitter.finagle.stream

import com.twitter.concurrent.{Channel => ConChannel, Observer, Serialized}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Cancelled, Ok, Error}
import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

/**
 * A Netty Channel Handler that adapts Twitter Channels to Netty Channels.
 * Note that a Twitter Channel is different than a Netty Channel despite having
 * the same name. A Twitter Channel is a uniplex, asynchronous intra-process
 * communication channel, whereas a Netty Channel typically represents
 * a duplex, socket-based comunication channel.
 */
class ChannelToHttpChunk extends SimpleChannelHandler {
  sealed abstract class State
  case object Idle extends State
  case object Open extends State
  case class Observing(observer: Observer) extends State
  private[this] val state = new AtomicReference[State](Idle)

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage match {
    case StreamResponse(httpResponse, twitterChannel) =>
      require(state.compareAndSet(Idle, Open), "Channel is already open or busy.")

      httpResponse.setChunked(true)
      HttpHeaders.setHeader(httpResponse, "Transfer-Encoding", "Chunked")

      val sendStartMessage = Channels.future(ctx.getChannel)
      Channels.write(ctx, sendStartMessage, httpResponse)
      sendStartMessage {
        case Ok(_) =>
          streamMessagesFromChannel(ctx, twitterChannel, e)
        case Cancelled =>
          e.getFuture.cancel()
        case Error(f) =>
          e.getFuture.setFailure(f)
      }

    case _ =>
      throw new IllegalArgumentException("Expecting a Channel" + e.getMessage)
  }

  private[this] def streamMessagesFromChannel(
    ctx: ChannelHandlerContext,
    channel: ConChannel[ChannelBuffer],
    e: MessageEvent)
  {
    channel.serialized {
      val observer = channel.respond { channelBuffer =>
        val messageFuture = Channels.future(ctx.getChannel)
        val result = messageFuture.toTwitterFuture
        Channels.write(ctx, messageFuture, new DefaultHttpChunk(channelBuffer))
        result
      }
      state.compareAndSet(Open, Observing(observer))
      channel.closes.respond { _ =>
        val closeFuture = e.getFuture
        state.set(Idle)
        Channels.write(ctx, closeFuture, new DefaultHttpChunkTrailer)
      }
    }
  }

  override def closeRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state.getAndSet(Idle) match {
      case Observing(observer) =>
        observer.dispose()
      case _ =>
    }
    super.closeRequested(ctx, e)
  }
}
