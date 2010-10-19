package com.twitter.netty.channel

import java.net.SocketAddress
import org.jboss.netty.channel.{Channel, ChannelFuture}

trait BrokeredAddress extends SocketAddress {
  def reserve(): ChannelFuture
  def release(channel: Channel)
}