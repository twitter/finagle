package com.twitter.finagle.channel

/** 
 * A ChannelSemaphoreHandler admits requests on a channel only after
 * acquiring a lease from the passed-in semaphore.
 */ 

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

import org.jboss.netty.channel._

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.AsyncSemaphore
import com.twitter.finagle.CodecException

class ChannelSemaphoreHandler(semaphore: AsyncSemaphore)
  extends SimpleChannelHandler
{
  private[this] sealed trait State
  private[this] case object Idle   extends State
  private[this] case object Busy   extends State
  private[this] case object Closed extends State

  private[this] def state(ctx: ChannelHandlerContext) = ctx.synchronized {
    if (ctx.getAttachment eq null)
      ctx.setAttachment(new AtomicReference[State](Idle))

    ctx.getAttachment.asInstanceOf[AtomicReference[State]]
  }

  private[this] def close(ctx: ChannelHandlerContext) {
    if (state(ctx).getAndSet(Closed) == Busy)
      semaphore.release()
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    semaphore.acquire {
      val ctxState = state(ctx)
      var oldState: State = null
      do {
        val state_ = ctxState.get
        if (state_ != Idle)
          oldState = state_
        if (ctxState.compareAndSet(Idle, Busy))
          oldState = Idle
      } while (oldState eq null)

      oldState match {
        case Idle =>
          super.messageReceived(ctx, e)
        case Busy =>
          semaphore.release()
          Channels.fireExceptionCaught(
            ctx.getChannel, new CodecException("Codec issued concurrent requests"))
        case Closed =>
          semaphore.release()
      }
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    super.writeRequested(ctx, e)
    e.getFuture onSuccessOrFailure {
      if (state(ctx).compareAndSet(Busy, Idle))
        semaphore.release()
    }
  }

  // Do we need to cover anything else?
  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    close(ctx)
    super.exceptionCaught(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    close(ctx)
    super.channelClosed(ctx, e)
  }

  override def closeRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    close(ctx)
    super.closeRequested(ctx, e)
  }
}
