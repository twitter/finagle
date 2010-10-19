package com.twitter.netty.channel

import org.jboss.netty.channel.{Channels, Channel}
import java.util.concurrent.ConcurrentLinkedQueue
import org.jboss.netty.bootstrap.ClientBootstrap

class PoolingBrokeredAddress(clientBootstrap: ClientBootstrap) extends BrokeredAddress {
  private val queue = new ConcurrentLinkedQueue[Channel]

  def reserve() = {
    var channel: Channel = null
    do {
      channel = queue.poll()
    } while ((channel ne null) && !channel.isConnected)

    if (channel ne null)
      Channels.succeededFuture(channel)
    else
      make()
  }

  def release(channel: Channel) {
    if (channel.isConnected)
      queue.offer(channel)
  }

  override def toString = "ChannelPool:%x".format(hashCode)

  private def make() = clientBootstrap.connect()
}