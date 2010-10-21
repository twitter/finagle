package com.twitter.finagle.channel

import java.net.SocketAddress
import org.jboss.netty.channel.{ChannelFuture, MessageEvent}

trait Broker extends SocketAddress {
  def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent)
}
