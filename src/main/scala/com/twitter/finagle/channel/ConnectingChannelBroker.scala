package com.twitter.finagle.channel

import java.nio.channels.ClosedChannelException

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._

import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._

trait ConnectingChannelBroker extends Broker {
  def getChannel: ChannelFuture
  def putChannel(channel: Channel)

  def dispatch(e: MessageEvent) = {
    val replyFuture = new ReplyFuture

    getChannel {
      case Ok(channel) if replyFuture.isCancelled =>
        putChannel(channel)

      case Ok(channel) =>
        // TODO: indicate failure to the pool here, or have it
        // determine that?
        connectChannel(channel, e, replyFuture) onSuccessOrFailure {
          putChannel(channel)
        }

      case Error(cause) =>
        replyFuture.setFailure(cause)

      case Cancelled =>
        replyFuture.setFailure(new CancelledRequestException)
    }

    replyFuture
  }

  protected def connectChannel(
      to: Channel, e: MessageEvent,
      replyFuture: ReplyFuture): ChannelFuture =
    to.getPipeline.getLast match {
      case adapter: BrokerAdapter =>
        adapter.writeAndRegisterReply(to, e, replyFuture)
      case _ =>
        val exc = new InvalidPipelineException
        e.getFuture().setFailure(exc)
        replyFuture.setFailure(exc)
        Channels.succeededFuture(to)
    }
}
