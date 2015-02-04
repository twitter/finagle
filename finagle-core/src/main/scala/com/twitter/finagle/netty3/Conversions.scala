package com.twitter.finagle.netty3

import org.jboss.netty.channel.ChannelFuture
import scala.language.implicitConversions

object Conversions {
  implicit def channelFutureToRichChannelFuture(f: ChannelFuture) = new RichChannelFuture(f)
}
