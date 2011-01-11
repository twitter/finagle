package com.twitter.finagle.channel

import org.jboss.netty.channel._

import com.twitter.util.{Promise, Future, Throw}

import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._

trait ConnectingChannelBroker extends Broker {
  def getChannel: ChannelFuture
  def putChannel(channel: Channel)

  def apply(request: AnyRef) = {
    val replyFuture = new Promise[AnyRef]

    getChannel {
      case Ok(channel) =>
        connectChannel(channel, request, replyFuture)
        replyFuture respond { _ =>
          // Note: The pool checks the health of the channel. The
          // ConnectingChannelBroker is responsible for the *request*
          // lifecycle, while the underlying pool handles the
          // *channel* lifecycle.
          putChannel(channel)
        }

      case Error(cause) =>
        // This is always a write failure.
        replyFuture() = Throw(new WriteException(cause))

      case Cancelled =>
        // This should never happen?  No code can currently cancel
        // these futures.
        replyFuture() = Throw(new CancelledRequestException)
    }

    replyFuture
  }

  private[this] def connectChannel(
      channel: Channel,
      message: AnyRef,
      replyFuture: Promise[AnyRef]) {
    channel.getPipeline.getLast match {
      case adapter: BrokerAdapter =>
        adapter.writeAndRegisterReply(channel, message, replyFuture)
      case _ =>
        replyFuture.updateIfEmpty(Throw(new InvalidPipelineException))
    }
  }
}

