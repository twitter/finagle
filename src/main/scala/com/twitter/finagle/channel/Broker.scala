package com.twitter.finagle.channel

import java.net.SocketAddress
import org.jboss.netty.channel.{Channel, ChannelFuture, MessageEvent, ChannelLocal}

object Broker {
  private val channelIdleLocal = new ChannelLocal[Boolean] {
    override def initialValue(channel: Channel) = true
  }

  def setChannelIdle(channel: Channel) {
    channelIdleLocal.set(channel, true)
  }

  def setChannelBusy(channel: Channel) {
    channelIdleLocal.set(channel, false)
  }

  def isChannelIdle(channel: Channel) =
    channelIdleLocal.get(channel)
}

trait Broker extends SocketAddress {
  def dispatch(e: MessageEvent): UpcomingMessageEvent
}
