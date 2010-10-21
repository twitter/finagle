package com.twitter.finagle.channel

import org.jboss.netty.channel._
import com.twitter.finagle.util.{Error, Ok}
import com.twitter.finagle.util.Conversions._

trait ChannelConnector extends ((BrokeredChannel, Channel, MessageEvent) => ChannelFuture)

object ConnectChannel extends ChannelConnector {
  def apply(from: BrokeredChannel, to: Channel, e: MessageEvent) = {
    to.getPipeline.addLast("handler", new ChannelConnectingHandler(from, to, e))
    val incomingWriteFuture = e.getFuture
    val writeFuture = Channels.write(to, e.getMessage)
    writeFuture {
      case Ok(channel) =>
        incomingWriteFuture.setSuccess()
      case Error(cause) =>
        incomingWriteFuture.setFailure(cause)
    }
    writeFuture
  }

  private class ChannelConnectingHandler(from: BrokeredChannel, to: Channel, e: MessageEvent)
    extends SimpleChannelUpstreamHandler
  {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      if (from.isOpen)
        Channels.fireMessageReceived(from, e.getMessage)
      to.getPipeline.remove(this)
    }
  }
}
