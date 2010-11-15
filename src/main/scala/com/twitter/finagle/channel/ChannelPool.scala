package com.twitter.finagle.channel

import collection.mutable.Queue

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channels, Channel}
import java.util.concurrent.ConcurrentLinkedQueue

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util._

class ChannelPool(clientBootstrap: BrokerClientBootstrap) extends Serialized
{
  private val channelQueue = new ConcurrentLinkedQueue[Channel]
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

  override def toString = "pool:%x".format(hashCode)
}

class ConnectionLimitingChannelPool(
  clientBootstrap: BrokerClientBootstrap,
  connectionLimit: Int)
  extends ChannelPool(clientBootstrap)
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
            channelFuture.setFailure(new CancelledConnectionException)
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
