package com.twitter.finagle.channel

import collection.mutable.Queue

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channels, Channel}
import java.util.concurrent.ConcurrentLinkedQueue

import com.twitter.util.{Time, Duration}
import com.twitter.util.TimeConversions._
import com.twitter.concurrent.Serialized

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util._

class ChannelPool(
  clientBootstrap: BrokerClientBootstrap,
  connectRetryPeriod: Option[Duration] = None,
  timer: Timer = Timer.default)
  extends Serialized
{
  @volatile private[this] var _isAvailable = false
  @volatile private[this] var lastConnectAttempt = Time.epoch

  private[this] val channelQueue = new ConcurrentLinkedQueue[Channel]

  // > TODO
  //     On boot, we attempt to connect. We set our availability state
  //     when successful. Currently we don't maintain such a heartbeat
  //     except for the initial connection attempt. We may consider
  //     keeping a core size, being unavailable unless the number of
  //     connections is strictly positive.
  //
  //     We may also consider backing off the connection retries.
  //     Note that this connection level health checking is unecessary
  //     if there exists more generic application level health checks,
  //     as an application would decidedly be unhealthy on connection
  //     failure.
  def tryToConnect(period: Duration) {
    val timeSinceLastConnectAttempt = lastConnectAttempt.untilNow

    if (timeSinceLastConnectAttempt < period) {
      timer.schedule(period - timeSinceLastConnectAttempt) {
        tryToConnect(period)
      }
    } else {
      lastConnectAttempt = Time.now
      reserve() {
        case Ok(channel) =>
          release(channel)
          _isAvailable = true
        case Cancelled =>
          tryToConnect(period)
        case Error(_) =>
          tryToConnect(period)
      }
    }
  }

  connectRetryPeriod match {
    case Some(period) => tryToConnect(period)
    case None => _isAvailable = true
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

  def isAvailable = _isAvailable

  protected def make() = clientBootstrap.connect()
  protected def isHealthy(channel: Channel) = channel.isOpen

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
