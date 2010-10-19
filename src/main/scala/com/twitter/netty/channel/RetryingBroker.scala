package com.twitter.netty.channel

import org.jboss.netty.channel.{DefaultChannelFuture, DownstreamMessageEvent, MessageEvent}
import com.twitter.netty.util.{Error, Ok}
import com.twitter.netty.util.Conversions._

class RetryingBroker(underlying: Broker, tries: Int) extends Broker {
  def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent) {
    dispatch(tries, handlingChannel, e)
  }

  def dispatch(triesLeft: Int, handlingChannel: BrokeredChannel, e: MessageEvent) {
    val incomingFuture = e.getFuture
    val interceptErrors = new DefaultChannelFuture(e.getChannel, false)
    interceptErrors {
      case Ok(channel) =>
        incomingFuture.setSuccess()
      case Error(cause) =>
        if (triesLeft > 0)
          dispatch(triesLeft - 1, handlingChannel, e)
        else
          incomingFuture.setFailure(cause)
    }

    val errorInterceptingMessageEvent = new DownstreamMessageEvent(
      e.getChannel,
      interceptErrors,
      e.getMessage,
      e.getRemoteAddress)

    underlying.dispatch(handlingChannel, errorInterceptingMessageEvent)
  }
}