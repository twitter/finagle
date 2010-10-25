package com.twitter.finagle.channel

import org.jboss.netty.channel._
import com.twitter.finagle.util.Conversions._

class UpcomingMessageEvent(channel: Channel, doneFuture: ChannelFuture) extends MessageEvent {
  def this(channel: Channel) = this(channel, Channels.future(channel))

  @volatile private var message: Option[AnyRef] = None
  @volatile private var next: Option[UpcomingMessageEvent] = None
  private val future = Channels.future(channel, true)

  def setNextMessage(message: AnyRef) = {
    val next = new UpcomingMessageEvent(channel, doneFuture)
    this.message = Some(message)
    this.next = Some(next)
    future.setSuccess()
    next
  }

  def setFinalMessage(message: AnyRef) {
    this.message = Some(message)
    future.setSuccess()
    doneFuture.setSuccess()
  }

  def setFailure(cause: Throwable) {
    future.setFailure(cause)
    doneFuture.setFailure(cause)
  }

  def cancel() {
    future.cancel()
  }

  // def onMessageReceived(f: MessageEvent)

  def getChannel: Channel = channel
  def getFuture: ChannelFuture = future
  def getDoneFuture: ChannelFuture = doneFuture
  def getMessage = message getOrElse null
  def getNext = next
  def getRemoteAddress = channel.getRemoteAddress
}

object UpcomingMessageEvent {
  def successfulEvent(channel: Channel, message: AnyRef) = {
    val e = new UpcomingMessageEvent(channel)
    e.setFinalMessage(message)
    e
  }

  def failedEvent(channel: Channel, cause: Throwable) = {
    val e = new UpcomingMessageEvent(channel)
    e.setFailure(cause)
    e
  }

}
