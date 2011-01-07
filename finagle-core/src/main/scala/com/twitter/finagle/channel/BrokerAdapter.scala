package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicReference

import org.jboss.netty.channel._

import com.twitter.finagle.util.Error
import com.twitter.finagle.util.Conversions._

import com.twitter.util.{Future, Promise, Return, Throw, Try}

class BrokerAdapter extends SimpleChannelUpstreamHandler {
  @volatile private[this] var replyFuture: Promise[AnyRef] = null

  def writeAndRegisterReply(to: Channel, message: AnyRef, replyFuture: Promise[AnyRef]) {
    if (this.replyFuture ne null) {
      done(Throw(new TooManyConcurrentRequestsException))
    } else {
      this.replyFuture = replyFuture
      Channels.write(to, message) {
        case Error(cause) =>
          // Always close on error.
          fail(to, new WriteException(cause))
        case _ => ()
      }
    }
  }

  // TODO: should we provide any event serialization here?

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
    done(Throw(new ChannelClosedException))
  }

  private[this] def fail(ch: Channel, cause: ChannelException) {
    // We always close the channel on failure, and mark ourselves
    // unhealthy.
    ChannelHealth.markUnhealthy(ch)
    Channels.close(ch)
    done(Throw(cause))
  }

  private[this] def done(answer: Try[AnyRef]) {
    if (replyFuture ne null) {
      replyFuture.updateIfEmpty(answer)
      replyFuture = null
    }
  }
}
