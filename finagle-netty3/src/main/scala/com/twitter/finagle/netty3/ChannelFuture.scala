package com.twitter.finagle.netty3

import org.jboss.netty.channel._

/**
 * A ChannelFuture that doesn't need to have a channel on creation.
 */
private[finagle] class LatentChannelFuture extends DefaultChannelFuture(null, false) {
  @volatile private var channel: Channel = _

  def setChannel(c: Channel) { channel = c }
  override def getChannel() = channel
}
