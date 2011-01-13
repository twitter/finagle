package com.twitter.finagle.channel

import org.jboss.netty.channel._

import com.twitter.finagle.util.Error
import com.twitter.finagle.util.Conversions._

import com.twitter.util.{Promise, Return, Throw, Try}

class BrokerAdapter extends SimpleChannelUpstreamHandler {
  @volatile private[this] var replyFuture: Promise[Any] = null

  def writeAndRegisterReply(
    channel: Channel, message: Any,
    incomingReplyFuture: Promise[Any])
  {
    // If there is an outstanding request, something up the stack has
    // messed up. We currently just fail this request immediately, and
    // let the current request complete.
    if (replyFuture ne null) {
      incomingReplyFuture.updateIfEmpty(Throw(new TooManyConcurrentRequestsException))
    } else {
      replyFuture = incomingReplyFuture
      Channels.write(channel, message) {
        case Error(cause) =>
          // Always close on error.
          fail(channel, new WriteException(cause))
        case _ => ()
      }
    }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    if (replyFuture eq null)
      fail(ctx.getChannel, new SpuriousMessageException)
    else
      done(Return(e.getMessage))
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    val translated =
      e.getCause match {
          case _: java.net.ConnectException =>
            new ConnectionFailedException
          case _: java.nio.channels.UnresolvedAddressException =>
            new ConnectionFailedException
          case e =>
            new UnknownChannelException(e)
        }

    // Always close on error.
    fail(ctx.getChannel, translated)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    fail(ctx.getChannel, new ChannelClosedException)
  }

  private[this] def fail(ch: Channel, cause: ChannelException) {
    // We always close the channel on failure.  This effectively
    // invalidates the channel.
    if (ch.isOpen) Channels.close(ch)
    done(Throw(cause))
  }

  private[this] def done(answer: Try[Any]) {
    if (replyFuture ne null) {
      // The order of operations here is important: the callback from
      // the future could invoke another request immediately, and
      // since the stack upstream knows we're done when the reply
      // future has been satisfied, it may reuse us immediately.
      val currentReplyFuture = replyFuture
      replyFuture = null
      currentReplyFuture.updateIfEmpty(answer)
    }
  }
}
