package com.twitter.finagle.netty4.channel

import com.twitter.finagle.transport.Transport
import io.netty.channel.{ChannelInitializer, Channel}
import io.netty.channel.ChannelHandler.Sharable

/**
 * Bridges a `Channel` onto a [[Transport]].
 */
@Sharable
private[netty4] class ServerBridge[In, Out](
    transportFac: Channel => Transport[In, Out],
    serveTransport: Transport[In, Out] => Unit)
  extends ChannelInitializer[Channel] {

  def initChannel(ch: Channel): Unit = {
    val transport: Transport[In, Out] = transportFac(ch)
    serveTransport(transport)
  }
}
