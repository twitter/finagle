package com.twitter.finagle.channel

import java.nio.channels.ClosedChannelException

import org.jboss.netty.channel.{
  Channels, Channel, MessageEvent, SimpleChannelUpstreamHandler,
  ChannelHandlerContext, ChannelStateEvent, ExceptionEvent,
  ChannelFuture}

import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._

trait ConnectingChannelBroker extends Broker {
  def getChannel: ChannelFuture
  def putChannel(channel: Channel)

  def dispatch(e: MessageEvent) = {
    val responseEvent = new UpcomingMessageEvent(e.getChannel)

    getChannel {
      case Ok(channel) =>
        if (responseEvent.getFuture.isCancelled) {
          putChannel(channel)
        } else {
          connectChannel(channel, e, responseEvent)
          responseEvent.getDoneFuture onSuccessOrFailure {
            putChannel(channel)
          }
        }
      case Error(cause) =>
        responseEvent.setFailure(cause)

      case Cancelled => ()
    }

    responseEvent
  }

  protected def connectChannel(to: Channel, e: MessageEvent, responseEvent: UpcomingMessageEvent) {
    val handler = new ChannelConnectingHandler(responseEvent, to, e)
    to.getPipeline.addLast("handler", handler)
    Channels.write(to, e.getMessage).proxyTo(e.getFuture)
  }

  private class ChannelConnectingHandler(
    responseEvent: UpcomingMessageEvent,
    to: Channel, e: MessageEvent)
    extends SimpleChannelUpstreamHandler
  {
    var currentResponseEvent = responseEvent

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      if (Broker.isChannelIdle(ctx.getChannel)) {
        currentResponseEvent.setFinalMessage(e.getMessage)
        to.getPipeline.remove(this)
      } else {
        currentResponseEvent =
          currentResponseEvent.setNextMessage(e.getMessage)
      }
    }

    // We rely on the underlying protocol handlers to close channels
    // on errors. (This is probably the only sane policy).
    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      currentResponseEvent.setFailure(new ClosedChannelException)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, exc: ExceptionEvent) {
      currentResponseEvent.setFailure(exc.getCause)
    }
  }
}
