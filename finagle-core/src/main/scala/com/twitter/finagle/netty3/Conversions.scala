package com.twitter.finagle.netty3

import org.jboss.netty.channel.ChannelFuture
import scala.language.implicitConversions

@deprecated("The ChannelFuture implicit conversions are deprecated. Please use `addListener` directly.")
object Conversions {
  implicit def channelFutureToRichChannelFuture(f: ChannelFuture) = new RichChannelFuture(f)
}
