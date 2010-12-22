package com.twitter.finagle.channel

import java.net.SocketAddress

import org.jboss.netty.channel._

// Represents a partial message.
case class PartialUpstreamMessageEvent(
    channel: Channel, message: AnyRef, remoteAddress: SocketAddress)
  extends UpstreamMessageEvent(channel, message, remoteAddress)

object PartialUpstreamMessageEvent {
  def apply(e: MessageEvent): PartialUpstreamMessageEvent =
    // Upstream messages have pre-satisfied futures.
    PartialUpstreamMessageEvent(e.getChannel, e.getMessage, e.getRemoteAddress)
}
