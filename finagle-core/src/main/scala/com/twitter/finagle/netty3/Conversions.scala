package com.twitter.finagle.netty3

import org.jboss.netty.channel.ChannelFuture

object Conversions {
  implicit def channelFutureToRichChannelFuture(f: ChannelFuture) = new RichChannelFuture(f)
}
