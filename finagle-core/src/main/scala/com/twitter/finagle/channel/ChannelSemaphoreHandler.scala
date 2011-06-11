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

// This is used as a no-op permit for the ChannelSemaphoreHandler.
private[channel] object DeadPermit extends Permit {
  def release() = ()
}

private[channel] object PlaceholderPermit extends Permit {
  def release() = ()
}
class ChannelSemaphoreHandler(semaphore: AsyncSemaphore)
  extends SimpleChannelHandler
{
  private[this] def waiter(ctx: ChannelHandlerContext) = ctx.synchronized {
    if (ctx.getAttachment eq null)
      ctx.setAttachment(new AtomicReference[Permit](null))
    ctx.getAttachment.asInstanceOf[AtomicReference[Permit]]
  }

  private[this] def close(ctx: ChannelHandlerContext) {
    // We substitute the permit with a dead one-- this causes subsequent 
    // permit releases to be no-ops, which is what we want.
    Option(waiter(ctx).getAndSet(DeadPermit)) foreach { _.release() }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    // Once a context transitions into DeadPermit, it never goes back.
    // We short cut messages from channels that have excepted or
    // closed.  It's possible to have a dead permit here because
    // messages may be (and are in the netty read loop) delivered
    // after exceptions are.
    if (waiter(ctx).get eq DeadPermit) return

    semaphore.acquire() onSuccess { permit =>
      if (waiter(ctx).compareAndSet(null, permit)) {
        super.messageReceived(ctx, e)
      } else {
        permit.release()
        if (waiter(ctx).get ne DeadPermit) {
          // Freak.  Out.  Note that this won't catch the case where
          // there is a race between more than one messageReceive
          // calls and an exception is interleaved.  This is o.k.
          // since the channel is anyway at that point dead.
          Channels.fireExceptionCaught(
            ctx.getChannel,
            new CodecException("Codec issued concurrent requests"))
        }
      }
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    val permit = waiter(ctx).getAndSet(null)
    if (permit eq null) {
      // This should only happen when a write is issued 
      // without a corresponding request.
      val exc = new CodecException("No waiter for downstream message!")
      Channels.fireExceptionCaught(ctx.getChannel, exc)

      // Don't propagate the write: it is invalid after all.
      e.getFuture.update(Error(exc))
      return
    }

    /**
     * Note: the permit at this point may well be dead (eg.  if the channel
     * has closed or had an exception.  However, this matters not: we simply
     * perform a no-op by releasing the dead permit, and the write will fail
     * anyway.  If we're writing on a truly dead channel, it's indeed the
     * fault of any upstream handler -- but we don't become inconsistent
     * because of it.
     */

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
