package com.twitter.finagle.channel

import org.jboss.netty.channel._
import com.twitter.finagle.util.Conversions._

class UpcomingMessageEvent(channel: Channel) extends MessageEvent {
  @volatile private var message: Option[AnyRef] = None
  private val future = Channels.future(channel, true)

  def setMessage(message: AnyRef) {
    this.message = Some(message)
    future.setSuccess()
  }

  def setFailure(cause: Throwable) {
    future.setFailure(cause)
  }

  def cancel() {
    future.cancel()
  }

  def getChannel: Channel = channel
  def getFuture: ChannelFuture = future

  def getMessage = message getOrElse null

  def getRemoteAddress = channel.getRemoteAddress
}

object UpcomingMessageEvent {
  def successfulEvent(channel: Channel, message: AnyRef) = {
    val e = new UpcomingMessageEvent(channel)
    e.setMessage(message)
    e
  }

  def failedEvent(channel: Channel, cause: Throwable) = {
    val e = new UpcomingMessageEvent(channel)
    e.setFailure(cause)
    e
  }

}
