package com.twitter.finagle.channel

import com.twitter.finagle.util.{Error, Ok}
import com.twitter.finagle.util.Conversions._
import org.jboss.netty.channel.{MessageEvent, Channels}

class PoolingBroker(channelPool: ChannelPool) extends Broker {
  def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent) {
    channelPool.reserve() {
      case Ok(channel) =>
        // XXX add a callback to release the connection.
        val handler = new BrokeredRequestHandler(handlingChannel)
        handler.write(channel, e)
      case Error(throwable) =>
        Channels.fireExceptionCaught(handlingChannel, throwable)
    }
  }
}
