package com.twitter.finagle.channel

/**
 * A ChannelSemaphoreHandler admits requests on a channel only after
 * acquiring a lease from the passed-in semaphore.
 */

import java.util.concurrent.atomic.AtomicReference

import org.jboss.netty.channel._

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.AsyncSemaphore
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.CodecException

class ChannelSemaphoreHandler(semaphore: AsyncSemaphore)
  extends SimpleChannelHandler
{
  private[this] def waiter(ctx: ChannelHandlerContext) = ctx.synchronized {
    if (ctx.getAttachment eq null)
      ctx.setAttachment(new AtomicReference[AsyncSemaphore#Permit](null))

    ctx.getAttachment.asInstanceOf[AtomicReference[AsyncSemaphore#Permit]]
  }

  private[this] def close(ctx: ChannelHandlerContext) {
    Option(waiter(ctx).getAndSet(null)) foreach { _.release() }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    semaphore.acquire() onSuccess { permit =>
      val oldPermit = waiter(ctx).getAndSet(permit)
      if (oldPermit ne null) {
        // Freak. Out.
        permit.release()
        oldPermit.release()
        waiter(ctx).set(null)
        Channels.fireExceptionCaught(
          ctx.getChannel, new CodecException("Codec issued concurrent requests"))
      } else {
        super.messageReceived(ctx, e)
      }
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    /**
     * We proxy the event here to ensure the correct ordering: we
     * need to update the upstream future *first* (so that listening
     * code may run), and only *then* release the semaphore. This
     * ensures correct ordering upstream.
     */
    val writeComplete = Channels.future(e.getChannel)
    val proxiedEvent = new DownstreamMessageEvent(
      e.getChannel, writeComplete,
      e.getMessage, e.getRemoteAddress)
    super.writeRequested(ctx, proxiedEvent)

    writeComplete { res =>
      // First let the upstream know about our write completion.
      e.getFuture.update(res)

      // Then release the permit, allowing for additional requests.
      val permit = waiter(ctx).getAndSet(null)
      if (permit eq null) {
        Channels.fireExceptionCaught(
          ctx.getChannel,
          new CodecException("No waiter for downstream message!"))
      } else {
        permit.release()
      }
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
