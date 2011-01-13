package com.twitter.finagle.channel

import com.twitter.finagle.util.Conversions._
import org.jboss.netty.channel._

class PoolingBroker[Req, Rep](channelPool: ChannelPool)
  extends ConnectingChannelBroker[Req, Rep]
{
  def getChannel = channelPool.reserve()
  def putChannel(channel: Channel) = channelPool.release(channel)
  def close() { channelPool.close() }
}
