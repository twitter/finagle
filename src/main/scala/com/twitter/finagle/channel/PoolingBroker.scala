package com.twitter.finagle.channel

import com.twitter.finagle.util.{Error, Ok}
import com.twitter.finagle.util.Conversions._
import org.jboss.netty.channel._

class PoolingBroker(channelPool: ChannelPool) extends Broker {
  def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent) {
    channelPool.reserve() {
      case Ok(channel) =>
        connectChannel(handlingChannel, channel, e) {
          case Ok(channel) =>
            channelPool.release(channel)
          case Error(cause) =>
            // XXX fireExceptionCaught untested
            channelPool.release(channel)
        }
      case Error(throwable) =>
        Channels.fireExceptionCaught(handlingChannel, throwable)
    }
  }

  private def connectChannel(from: BrokeredChannel, to: Channel, e: MessageEvent) = {
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
