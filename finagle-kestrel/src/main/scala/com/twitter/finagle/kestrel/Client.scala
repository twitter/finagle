package com.twitter.finagle.kestrel

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.{Future, Duration, Time, Return, Throw}
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.concurrent.{Channel, Topic}

object Client {
  def apply(raw: Service[Command, Response]): Client = {
    new ConnectedClient(raw)
  }
}

/**
 * A friendly Kestrel client Interface.
 */
trait Client {
  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch): Future[Response]
  def get(queueName: String, waitUpTo: Duration = 0.seconds): Future[Option[ChannelBuffer]]
  def delete(queueName: String): Future[Response]
  def flush(queueName: String): Future[Response]
  def channel(queueName: String, waitUpTo: Duration = 0.seconds): Channel[ChannelBuffer]
}

/**
 * A Client representing a single TCP connection to a single server.
 *
 * @param  underlying  a Service[Command, Response]. '''Note:''' underlying MUST not use a connection pool or load-balance!
 */
protected class ConnectedClient(underlying: Service[Command, Response]) extends Client {
  def flush(queueName: String) = {
    underlying(Flush(queueName))
  }

  def delete(queueName: String) = {
    underlying(Delete(queueName))
  }

  /**
   * Enqueue an item.
   *
   * @param  expiry  how long the item is valid for (Kestrel will delete the item if it isn't dequeued in time.
   */
  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch) = {
    underlying(Set(queueName, expiry, value))
  }

  /**
   * Dequeue an item.
   *
   * @param  waitUpTo  if the queue is empty, indicate to the Kestrel server how long to block the operation, waiting for something to arrive, before returning None
   */
  def get(queueName: String, waitUpTo: Duration = 0.seconds) = {
    underlying(Get(queueName, collection.Set(Timeout(waitUpTo)))) map {
      case Values(Seq()) => None
      case Values(Seq(Value(key, value))) => Some(value)
    }
  }

  /**
   * Get a channel for the given queue
   *
   * @return  A Channel object that you can receive items from as they arrive.
   */
  def channel(queueName: String, waitUpTo: Duration = 10.seconds): Channel[ChannelBuffer] = {
    val channel = new Topic[ChannelBuffer]
    channel.onReceive {
      receive(queueName, channel, waitUpTo, collection.Set(Open()))
    }
    channel
  }

  private[this] def receive(queueName: String, channel: Topic[ChannelBuffer], waitUpTo: Duration, options: collection.Set[GetOption]) {
    if (channel.isOpen) {
      underlying(Get(queueName, collection.Set(Timeout(waitUpTo)) ++ options)) respond {
        case Return(Values(Seq(Value(key, item)))) =>
          try {
            channel.send(item)
            receive(queueName, channel, waitUpTo, collection.Set(Close(), Open()))
          } catch {
            case e =>
              underlying(Get(queueName, collection.Set(Abort())))
              channel.close()
          }
        case Return(Values(Seq())) =>
          receive(queueName, channel, waitUpTo, collection.Set(Open()))
        case Throw(e) =>
          channel.close()
      }
    }
  }
}
