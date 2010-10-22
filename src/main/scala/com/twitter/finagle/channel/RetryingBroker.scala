package com.twitter.finagle.channel

import org.jboss.netty.channel.{Channels, DownstreamMessageEvent, MessageEvent}
import com.twitter.finagle.util.{Cancelled, Error, Ok}
import com.twitter.finagle.util.Conversions._

class RetryingBroker(underlying: Broker, tries: Int) extends Broker {
  def dispatch(e: MessageEvent) = dispatch(tries, e)

  def dispatch(triesLeft: Int, e: MessageEvent): UpcomingMessageEvent = {
    val incomingFuture = e.getFuture
    val interceptErrors = Channels.future(e.getChannel)
    interceptErrors {
      case Ok(channel) =>
        incomingFuture.setSuccess()
      case Error(cause) =>
        // TODO: distinguish between *retriable* cause and non?
        if (triesLeft > 1)
          dispatch(triesLeft - 1, e)
        else
          incomingFuture.setFailure(cause)
    }

    val errorInterceptingMessageEvent = new DownstreamMessageEvent(
      e.getChannel,
      interceptErrors,
      e.getMessage,
      e.getRemoteAddress)

    underlying.dispatch(errorInterceptingMessageEvent)
  }
}
