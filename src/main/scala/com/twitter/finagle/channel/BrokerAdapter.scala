package com.twitter.finagle.channel

import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicReference

import org.jboss.netty.channel._

import com.twitter.finagle.util.Conversions._

class BrokerAdapter extends SimpleChannelUpstreamHandler {
  val currentReplyFuture = new AtomicReference[ReplyFuture](null)
  @volatile var doneFuture: ChannelFuture = null

  def writeAndRegisterReply(to: Channel, e: MessageEvent, replyFuture: ReplyFuture) = {
    if (!currentReplyFuture.compareAndSet(null, replyFuture))
      throw new TooManyDicksOnTheDanceFloorException

    doneFuture = Channels.future(e.getChannel)
    Channels.write(to, e.getMessage).proxyTo(e.getFuture)
    doneFuture
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val replyFuture = currentReplyFuture.get()
    if (replyFuture eq null)
      return  // TODO: log/change health?

    e match {
      case PartialUpstreamMessageEvent(_, message, _) =>
        val next = new ReplyFuture
        replyFuture.setReply(Reply.More(message, next))
        assert(currentReplyFuture.compareAndSet(replyFuture, next))
      case _ =>
        replyFuture.setReply(Reply.Done(e.getMessage))
        done()
    }
  }

  def done() {
    currentReplyFuture.set(null)
    doneFuture.setSuccess()
  }

  def fail(cause: Throwable) {
    val replyFuture = currentReplyFuture.get
    if (replyFuture eq null)
      return  // TODO: report?

    replyFuture.setFailure(cause)
    done()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    fail(e.getCause)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    fail(new ClosedChannelException)
  }
}
