package com.twitter.finagle.channel

import com.twitter.finagle.util.{Error, Ok, Cancelled}
import com.twitter.finagle.util.Conversions._
import org.jboss.netty.channel._

class PoolingBroker(channelPool: ChannelPool) extends ConnectingChannelBroker {
  override def isAvailable = channelPool.isAvailable // XXX && super.isAvailable

  def getChannel = channelPool.reserve()
  def putChannel(channel: Channel) = channelPool.release(channel)
}
