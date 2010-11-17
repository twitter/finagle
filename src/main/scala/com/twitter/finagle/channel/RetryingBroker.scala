package com.twitter.finagle.channel

import java.net.SocketAddress
import java.util.concurrent.TimeUnit

import org.jboss.netty.channel.{
  Channels, Channel, DownstreamMessageEvent,
  MessageEvent, ChannelFuture}
import org.jboss.netty.util.{HashedWheelTimer, TimerTask, Timeout}

import com.twitter.finagle.util.{Cancelled, Error, Ok}
import com.twitter.finagle.util.Conversions._

import com.twitter.util.TimeConversions._
import com.twitter.util.Duration

trait RetryingBrokerBase extends Broker {
  def retryFuture(channel: Channel): ChannelFuture
  val underlying: Broker

  class WrappingMessageEvent(channel: Channel, future: ChannelFuture, message: AnyRef, remoteAddress: SocketAddress)
    extends MessageEvent
  {
    override def getRemoteAddress = remoteAddress
    override def getFuture = future
    override def getMessage = message
    override def getChannel = channel
  }

  def dispatch(e: MessageEvent): ReplyFuture = {
    val incomingFuture = e.getFuture
    val interceptErrors = Channels.future(e.getChannel)
    interceptErrors {
      case Ok(channel) =>
        incomingFuture.setSuccess()
      case Error(cause) =>
        // TODO: distinguish between *retriable* cause and non?
        println("Received an error: %s".format(cause))
        retryFuture(e.getChannel) {
          case Ok(_) => dispatch(e)
          case _ => incomingFuture.setFailure(cause)
        }

      case Cancelled =>
        incomingFuture.cancel()
    }

    val errorInterceptingMessageEvent = new WrappingMessageEvent(
      e.getChannel,
      interceptErrors,
      e.getMessage,
      e.getRemoteAddress)

    underlying.dispatch(errorInterceptingMessageEvent)
  }
}

class RetryingBroker(val underlying: Broker, tries: Int) extends RetryingBrokerBase {
  @volatile var triesLeft = tries
  def retryFuture(channel: Channel) = {
    triesLeft -= 1
    if (triesLeft > 0)
      Channels.succeededFuture(channel)
    else
      Channels.failedFuture(channel, new RetryFailureException)
  }
}

// TODO: we need to make a version that also retries in the middle of
// a reply (streaming).

object ExponentialBackoffRetryingBroker {
  // the default tick is 100ms
  val timer = new HashedWheelTimer()
}

// TODO: max cap.

class ExponentialBackoffRetryingBroker(val underlying: Broker, initial: Duration, multiplier: Int)
 extends RetryingBrokerBase
{
  import ExponentialBackoffRetryingBroker._

  @volatile var delay = initial

  def retryFuture(channel: Channel) = {
    val future = Channels.future(channel)

    timer.newTimeout(new TimerTask {
      def run(to: Timeout) {
        ExponentialBackoffRetryingBroker.this.delay *= multiplier
        future.setSuccess()
      }
    }, delay.inMilliseconds, TimeUnit.MILLISECONDS)

    future
  }
}
