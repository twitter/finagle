package com.twitter.finagle.channel

import collection.mutable.Queue

import org.jboss.netty.channel.{Channels, Channel}
import java.util.concurrent.ConcurrentLinkedQueue

import com.twitter.util.{Time, Duration}
import com.twitter.util.TimeConversions._
import com.twitter.concurrent.Serialized

import com.twitter.finagle.CancelledRequestException
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util._

class ChannelPool(
  clientBootstrap: BrokerClientBootstrap,
  connectRetryPeriod: Option[Duration] = None,
  timer: Timer = Timer.default)
  extends Serialized
{
  private[this] val channelQueue = new ConcurrentLinkedQueue[Channel]

  connectRetryPeriod match {
    case Some(period) => tryToConnect(period)
    case None =>
  }

  // > TODO
  //     On boot, we attempt to connect. Currently we don't maintain such a heartbeat
  //     except for the initial connection attempt. We may consider
  //     keeping a core size, being unavailable unless the number of
  //     connections is strictly positive.
  //
  //     We may also consider backing off the connection retries.
  //     Note that this connection level health checking is unecessary
  //     if there exists more generic application level health checks,
  //     as an application would decidedly be unhealthy on connection
  //     failure.
  private[this] def tryToConnect(period: Duration) {
    reserve() {
      case Ok(channel) =>
        release(channel)
      case Cancelled =>
        timer.schedule(period.fromNow) { tryToConnect(period) }
      case Error(_) =>
        timer.schedule(period.fromNow) { tryToConnect(period) }
    }
  }

  protected def enqueue(channel: Channel) { channelQueue offer channel }
  protected def dequeue() = {
    var channel: Channel = null
    do {
      channel = channelQueue.poll()
    } while ((channel ne null) && !isHealthy(channel))
    channel
  }

  def reserve() = {
    val channel = dequeue()
    if (channel ne null)
      Channels.succeededFuture(channel)
    else
      make()
  }

  def release(channel: Channel) {
    if (isHealthy(channel)) enqueue(channel)
  }

  protected def make() = clientBootstrap.connect()
  protected def isHealthy(channel: Channel) = channel.isOpen

  def close() {}

  override def toString = "pool:%x".format(hashCode)
}

class ConnectionLimitingChannelPool(
    clientBootstrap: BrokerClientBootstrap,
    connectionLimit: Int,
    connectRetryPeriod: Option[Duration] = None)
  extends ChannelPool(clientBootstrap, connectRetryPeriod)
{
  private var connectionsMade = 0
  private val awaitingChannelQueue = new Queue[LatentChannelFuture]

  override def make() = {
    val channelFuture = new LatentChannelFuture

    serialized {
      if (connectionsMade < connectionLimit) {
        connectionsMade += 1

        super.make() {
          case Ok(channel) =>
            channelFuture.setChannel(channel)
            channelFuture.setSuccess()
          case Error(cause) =>
            channelFuture.setFailure(cause)
            serialized { connectionsMade -= 1 }
          case Cancelled =>
            channelFuture.setFailure(new CancelledRequestException)
            serialized { connectionsMade -= 1 }
        }
      } else {
        awaitingChannelQueue += channelFuture
      }

      ()
    }

    channelFuture
  }

  override def release(channel: Channel) {
    val channelIsHealthy = isHealthy(channel)

    serialized {
      if (!channelIsHealthy) {
        // Drop the channel, make room for another connection.
        connectionsMade -= 1
      } else {
        if (awaitingChannelQueue isEmpty) {
          enqueue(channel)
        } else {
          val channelFuture = awaitingChannelQueue.dequeue
          channelFuture.setChannel(channel)
          channelFuture.setSuccess()
        }
      }

      ()
    }
  }
}
