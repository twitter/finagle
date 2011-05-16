package com.twitter.finagle.channel

/**
 * A ChannelSemaphoreHandler admits requests on a channel only after
 * acquiring a lease from the passed-in semaphore.
 */

import java.util.concurrent.atomic.AtomicReference

import org.jboss.netty.channel._

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.CodecException

import com.twitter.concurrent.{AsyncSemaphore, Permit}

class ChannelSemaphoreHandler(semaphore: AsyncSemaphore)
  extends SimpleChannelHandler
{
  private[this] def waiter(ctx: ChannelHandlerContext) = ctx.synchronized {
    if (ctx.getAttachment eq null)
      ctx.setAttachment(new AtomicReference[Permit](null))
    ctx.getAttachment.asInstanceOf[AtomicReference[Permit]]
  }

  private[this] def close(ctx: ChannelHandlerContext) {
    // Putting null here only causes exceptions when a write is requested after the conn is closed.
    Option(waiter(ctx).getAndSet(null)) foreach { _.release() }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    semaphore.acquire() onSuccess { permit =>
      if (waiter(ctx).compareAndSet(null, permit)) {
        super.messageReceived(ctx, e)
      } else {
        // Freak. Out.
        // Don't release the permit that we didn't acquire.
        permit.release()
        Channels.fireExceptionCaught(
          ctx.getChannel,
          new CodecException("Codec issued concurrent requests"))
      }
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    val permit = waiter(ctx).getAndSet(null)
    if (permit eq null) {
      /**
      * The circumstances that the permit is null are:
      *  - A write was issued without a corresponding request.
      *  - The connection has closed or an exception was thrown,
      *    in which case this error is still accurate.
      */
      Channels.fireExceptionCaught(
        ctx.getChannel,
        new CodecException("No waiter for downstream message!"))
    }

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

    /**
    * We need to attach this here because we want to release our permit before
    * another event does something like close the connection.
    */
    writeComplete { res =>
      try {
        // First let the upstream know about our write completion.
        e.getFuture.update(res)
      } finally {
        // Then release the permit, allowing for additional requests.
        permit.release()
      }
    }

    super.writeRequested(ctx, proxiedEvent)
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
