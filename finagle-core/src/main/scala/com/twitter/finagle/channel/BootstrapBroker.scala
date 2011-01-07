package com.twitter.finagle.channel

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channels, Channel, MessageEvent}

import com.twitter.finagle.util.{Ok, Error}
import com.twitter.finagle.util.Conversions._

class BootstrapBroker(bootstrap: BrokerClientBootstrap)
  extends ConnectingChannelBroker
{
  if (bootstrap.getOption("remoteAddress") eq null)
    throw new IllegalArgumentException("bootstrap remoteAddress is required")

  def getChannel = bootstrap.connect()
  def putChannel(channel: Channel) = close(channel)

  private[channel] def close(channel: Channel) {
    Channels.close(channel)
  }
}
