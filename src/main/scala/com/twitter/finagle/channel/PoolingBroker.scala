package com.twitter.finagle.channel

import java.nio.channels.ClosedChannelException

import com.twitter.finagle.util.{Error, Ok, Cancelled}
import com.twitter.finagle.util.Conversions._
import org.jboss.netty.channel._

class PoolingBroker(channelPool: ChannelPool) extends Broker {
  def dispatch(e: MessageEvent) = {
    val responseEvent = new UpcomingMessageEvent(e.getChannel)

    channelPool.reserve() {
      case Ok(channel) =>
        if (responseEvent.getFuture.isCancelled) {
          channelPool.release(channel)
        } else {
          connectChannel(channel, e, responseEvent)
          responseEvent.getFuture onSuccessOrFailure { channelPool.release(channel) }
        }
      case Error(cause) =>
        responseEvent.setFailure(cause)

      case Cancelled => ()
    }

    responseEvent
  }

  private def connectChannel(to: Channel, e: MessageEvent, responseEvent: UpcomingMessageEvent) {
    val handler = new ChannelConnectingHandler(responseEvent, to, e)
    to.getPipeline.addLast("handler", handler)
    Channels.write(to, e.getMessage).proxyTo(e.getFuture)
  }

  private class ChannelConnectingHandler(
    responseEvent: UpcomingMessageEvent,
    to: Channel, e: MessageEvent)
    extends SimpleChannelUpstreamHandler
  {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      responseEvent.setMessage(e.getMessage)
      to.getPipeline.remove(this)
    }

    // We rely on the underlying protocol handlers to close channels
    // on errors. (This is probably the only sane policy).
    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      responseEvent.setFailure(new ClosedChannelException)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, exc: ExceptionEvent) {
      responseEvent.setFailure(exc.getCause)
    }
  }
}
