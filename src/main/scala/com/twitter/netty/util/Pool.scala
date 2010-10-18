package com.twitter.netty.util

import java.util.concurrent.ConcurrentLinkedQueue
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channels, Channel, ChannelFuture}

trait ChannelPool {
  private val queue = new ConcurrentLinkedQueue[Channel]

  /**
   * Return a future for channel connection.
   */
  protected def make(): ChannelFuture

  def get(): ChannelFuture = {
    var channel: Channel = null
    do {
      channel = queue.poll()
    } while ((channel ne null) && !channel.isConnected)

    if (channel ne null)
      return Channels.succeededFuture(channel)
    else
      make()
  }

  def put(channel: Channel) {
    if (channel.isConnected)
      queue.offer(channel)
  }

  override def toString = "ChannelPool:%x".format(hashCode)
}

class BootstrappedChannelPool(bootstrap: ClientBootstrap) extends ChannelPool {
  def make() = bootstrap.connect()
}