package com.twitter.netty.channel

import org.jboss.netty.channel._
import com.twitter.netty.util.{Error, Ok}
import com.twitter.netty.util.Conversions._

class BrokeredRequestHandler(pairedChannel: BrokeredChannel)
  extends SimpleChannelUpstreamHandler
{
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    if (pairedChannel.isOpen)
      Channels.fireMessageReceived(pairedChannel, e.getMessage)
    detach(ctx.getChannel)
  }

  def write(channel: Channel, e: MessageEvent) {
    channel.getPipeline.addLast("handler", this)
    val incomingWriteFuture = e.getFuture
    Channels.write(channel, e.getMessage) {
      case Ok(channel) =>
        incomingWriteFuture.setSuccess()
      case Error(cause) =>
        incomingWriteFuture.setFailure(cause)
    }
  }

  private def detach(channel: Channel) {
    channel.getPipeline.remove(this)
  }
}
