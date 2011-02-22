package com.twitter.finagle.stream

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.channel._
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import com.twitter.concurrent.Observer
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.Ok
import org.jboss.netty.util.CharsetUtil

/**
 * A Netty Channel Handler that adapts Twitter Channels to Netty Channels.
 * Note that a Twitter Channel is unlike a Netty Channel despite having
 * the same name. A Twitter Channel is a uniplex, asynchronous intra-process
 * communication channel, whereas a Netty Channel typically represents
 * a duplex, socket-based comunication channel.
 */
class ChannelToHttpChunk extends SimpleChannelDownstreamHandler {
  sealed abstract class State
  case object Idle extends State
  case object Open extends State
  case class Observing(observer: Observer) extends State

  private[this] val state = new AtomicReference[State](Idle)

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage match {
    case channel: com.twitter.concurrent.Channel[ChannelBuffer] =>
      require(state.compareAndSet(Idle, Open), "Channel is already open or busy.")

      val startMessage = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      HttpHeaders.setHeader(startMessage, "Transfer-Encoding", "Chunked")

      val startFuture = new DefaultChannelFuture(ctx.getChannel, false)
      Channels.write(ctx, startFuture, startMessage)
      startFuture {
        case Ok(_) =>
          val observer = channel.respond(this) { channelBuffer =>
            val messageFuture = new DefaultChannelFuture(ctx.getChannel, false)
            val result = messageFuture.toTwitterFuture
            Channels.write(ctx, messageFuture, new DefaultHttpChunk(channelBuffer))
            result
          }
          state.set(Observing(observer))
          channel.closes.respond { _ =>
            val closeFuture = new DefaultChannelFuture(ctx.getChannel, false)
            val result = closeFuture.toTwitterFuture
            state.set(Idle)
            Channels.write(ctx, closeFuture, new DefaultHttpChunkTrailer)
            result
          }
        case _ =>
      }

    case _ =>
      throw new IllegalArgumentException("Expecting a Channel" + e.getMessage)
  }

  override def closeRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state.getAndSet(Idle) match {
      case Observing(observer) => observer.dispose
      case _ =>
    }
  }
}