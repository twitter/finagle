package com.twitter.netty.channel

import com.twitter.netty.util.{Error, Ok}
import com.twitter.netty.util.Conversions._
import org.jboss.netty.channel.{MessageEvent, Channels}

class PoolingBroker(channelPool: ChannelPool) extends Broker {
  def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent) {
    channelPool.reserve() {
      case Ok(channel) =>
        val handler = new BrokeredRequestHandler(handlingChannel)
        handler.write(channel, e)
      case Error(throwable) =>
        Channels.fireExceptionCaught(handlingChannel, throwable)
    }
  }
}
