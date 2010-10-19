package com.twitter.finagle.channel

import java.net.SocketAddress
import org.jboss.netty.channel.MessageEvent

trait Broker extends SocketAddress {
  def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent)
}
