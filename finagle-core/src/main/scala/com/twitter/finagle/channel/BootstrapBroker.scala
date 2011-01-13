package com.twitter.finagle.channel

import org.jboss.netty.channel.{Channels, Channel}

import com.twitter.finagle.util.Conversions._

class BootstrapBroker[Rep, Req](bootstrap: BrokerClientBootstrap)
  extends ConnectingChannelBroker[Rep, Req]
{
  require(bootstrap.getOption("remoteAddress") ne null,
    "bootstrap remoteAddress is required")
  def getChannel = bootstrap.connect()
  def putChannel(channel: Channel) = close(channel)

  protected[channel] def close(channel: Channel) {
    Channels.close(channel)
  }

  def close() {}
}
