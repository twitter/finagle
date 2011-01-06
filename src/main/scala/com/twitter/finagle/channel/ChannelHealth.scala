package com.twitter.finagle.channel

import org.jboss.netty.channel._

/**
 * ChannelHealth is a ChannelLocal that stores channel-healthy state,
 * allowing for consumers to mark channels as appropriate.
 */
object ChannelHealth extends ChannelLocal[Boolean] {
  override def initialValue(ch: Channel) = true

  // TODO: include isOpen here?

  def markHealthy(ch: Channel) { set(ch, true) }
  def markUnhealthy(ch: Channel) { set(ch, false) }
  def isHealthy(ch: Channel) = get(ch)
}
