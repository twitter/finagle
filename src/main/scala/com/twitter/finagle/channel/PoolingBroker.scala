package com.twitter.finagle.channel

import com.twitter.finagle.util.{Error, Ok}
import com.twitter.finagle.util.Conversions._
import org.jboss.netty.channel.{MessageEvent, Channels}

class PoolingBroker(connectChannel: ChannelConnector, channelPool: ChannelPool) extends Broker {
  def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent) {
    channelPool.reserve() {
      case Ok(channel) =>
        connectChannel(handlingChannel, channel, e) {
          case Ok(channel) =>
            channelPool.release(channel)
          case Error(cause) =>
            channelPool.release(channel)
        }
      case Error(throwable) =>
        Channels.fireExceptionCaught(handlingChannel, throwable)
    }
  }
}
