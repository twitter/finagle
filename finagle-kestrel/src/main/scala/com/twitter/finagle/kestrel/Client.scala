package com.twitter.finagle.kestrel

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.{Future, Duration, Time, Return, Throw}
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.concurrent.{ChannelSource, Channel}
import com.twitter.finagle.builder.ClientBuilder

object Client {
  def apply(raw: Service[Command, Response]): Client = {
    new ConnectedClient(raw)
  }

  def apply(hosts: String): Client = {
    val service = ClientBuilder()
      .codec(new Kestrel)
      .hosts(hosts)
      .build()
    apply(service)
  }
}

/**
 * A friendly Kestrel client Interface.
 */
trait Client {
  /**
   * Enqueue an item.
   *
   * @param  expiry  how long the item is valid for (Kestrel will delete the item if it isn't dequeued in time.
   */
  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch): Future[Response]

  /**
   * Dequeue an item.
   *
   * @param  waitUpTo  if the queue is empty, indicate to the Kestrel server how long to block the operation, waiting for something to arrive, before returning None
   */
  def get(queueName: String, waitUpTo: Duration = 0.seconds): Future[Option[ChannelBuffer]]

  /**
   * Delete a queue. Removes the journal file on the remote server.
   */
  def delete(queueName: String): Future[Response]

  /**
   * Flush a queue. Empties all items from the queue without deleting the journal.
   */
  def flush(queueName: String): Future[Response]

  /**
   * Get a channel for the given queue
   *
   * @return A Channel object that you can receive items from as they arrive.
   */
  def sink(queueName: String, waitUpTo: Duration = 0.seconds): Channel[ChannelBuffer]

  /**
   * Get a ChannelSource for the given queue
   *
   * @return  A ChannelSource that you can send items to.
   */
  def source(queueName: String): ChannelSource[ChannelBuffer]
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

  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch) = {
    underlying(Set(queueName, expiry, value))
  }

  def get(queueName: String, waitUpTo: Duration = 0.seconds) = {
    underlying(Get(queueName, collection.Set(Timeout(waitUpTo)))) map {
      case Values(Seq()) => None
      case Values(Seq(Value(key, value))) => Some(value)
    }
  }

  def sink(queueName: String, waitUpTo: Duration = 10.seconds): Channel[ChannelBuffer] = {
    val sink = new ChannelSource[ChannelBuffer]
    sink.responds.first.foreach { _ =>
      println("a responder registered...")
      receive(queueName, sink, waitUpTo, collection.Set(Open()))
    }
    sink
  }

  def source(queueName: String): ChannelSource[ChannelBuffer] = {
    val source = new ChannelSource[ChannelBuffer]
    source.respond(source) { item =>
      println("putting item==")
      set(queueName, item)
    }
    source
  }

  private[this] def receive(queueName: String, channel: ChannelSource[ChannelBuffer], waitUpTo: Duration, options: collection.Set[GetOption]) {
    if (channel.isOpen) {
      println("calling GET")
      underlying(Get(queueName, collection.Set(Timeout(waitUpTo)) ++ options)) respond {
        case Return(Values(Seq(Value(key, item)))) =>
          println("getsting an item===")
          try {
            channel.send(item)
            receive(queueName, channel, waitUpTo, collection.Set(Close(), Open()))
          } catch {
            case e =>
              underlying(Get(queueName, collection.Set(Abort())))
              channel.close()
          }
        case Return(Values(Seq())) =>
          println("getting item==")
          receive(queueName, channel, waitUpTo, collection.Set(Open()))
        case Throw(e) =>
          e.printStackTrace()
          println("error")
          channel.close()
      }
    }
  }
}
