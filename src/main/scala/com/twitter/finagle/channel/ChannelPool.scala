package com.twitter.finagle.channel

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channels, Channel}
import java.util.concurrent.ConcurrentLinkedQueue

class ChannelPool(clientBootstrap: ClientBootstrap) {
  private val queue = new ConcurrentLinkedQueue[Channel]
  
  def reserve() = {
    var channel: Channel = null
    do {
      channel = queue.poll()
    } while ((channel ne null) && !isHealthy(channel))

    if (channel ne null)
      Channels.succeededFuture(channel)
    else
      make()
  }

  def release(item: Channel) {
    if (isHealthy(item)) queue.offer(item)
  }

  private def make() = clientBootstrap.connect()
  private def isHealthy(channel: Channel) = channel.isOpen

  override def toString = "Pool:%x".format(hashCode)
}
