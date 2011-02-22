package com.twitter.finagle.kestrel

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.{Future, Duration, Time, Return, Throw}
import com.twitter.conversions.time._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.concurrent.{ChannelSource, Channel}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.{ServiceFactory, Service}

object Client {
  def apply(raw: ServiceFactory[Command, Response]): Client = {
    new ConnectedClient(raw)
  }

  def apply(hosts: String): Client = {
    val service = ClientBuilder()
      .codec(new Kestrel)
      .hosts(hosts)
      .buildFactory()
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
  def from(queueName: String, waitUpTo: Duration = 0.seconds): Channel[ChannelBuffer]

  /**
   * Get a ChannelSource for the given queue
   *
   * @return  A ChannelSource that you can send items to.
   */
  def to(queueName: String): ChannelSource[ChannelBuffer]
}

/**
 * A Client representing a single TCP connection to a single server.
 *
 * @param  underlying  a ServiceFactory[Command, Response].
 */
protected class ConnectedClient(underlying: ServiceFactory[Command, Response]) extends Client {
  def flush(queueName: String) = {
    underlying.service(Flush(queueName))
  }

  def delete(queueName: String) = {
    underlying.service(Delete(queueName))
  }

  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch) = {
    underlying.service(Set(queueName, expiry, value))
  }

  def get(queueName: String, waitUpTo: Duration = 0.seconds) = {
    underlying.service(Get(queueName, collection.Set(Timeout(waitUpTo)))) map {
      case Values(Seq()) => None
      case Values(Seq(Value(key, value))) => Some(value)
      case _ => throw new IllegalArgumentException
    }
  }

  def from(queueName: String, waitUpTo: Duration = 10.seconds): Channel[ChannelBuffer] = {
    val serviceFuture = underlying.make()
    val from = new ChannelSourceWithService(serviceFuture)
    serviceFuture foreach { service =>
      from.responds.first.foreach { _ =>
        receive(service, queueName, from, waitUpTo, collection.Set(Open()))
      }
    }
    from
  }

  def to(queueName: String): ChannelSource[ChannelBuffer] = {
    val to = new ChannelSource[ChannelBuffer]
    to.respond(to) { item =>
      Future { set(queueName, item) }
    }
    to
  }

  private[this] def receive(
    service: Service[Command, Response],
    queueName: String,
    channel: ChannelSource[ChannelBuffer],
    waitUpTo: Duration,
    options: collection.Set[GetOption])
  {
    if (channel.isOpen) {
      service(Get(queueName, collection.Set(Timeout(waitUpTo)) ++ options)) respond {
        case Return(Values(Seq(Value(key, item)))) =>
          try {
            channel.send(item)
            receive(service, queueName, channel, waitUpTo, collection.Set(Close(), Open()))
          } catch {
            case e =>
              service(Get(queueName, collection.Set(Abort())))
              channel.close()
          }
        case Return(Values(Seq())) =>
          receive(service, queueName, channel, waitUpTo, collection.Set(Open()))
        case Throw(e) =>
          e.printStackTrace()
          channel.close()
        case _ => throw new IllegalArgumentException
      }
    }
  }

  private[this] class ChannelSourceWithService(serviceFuture: Future[Service[Command, Response]]) extends ChannelSource[ChannelBuffer] {
    serviceFuture handle { e =>
      e.printStackTrace()
      this.close()
    }

    override def close() {
      try {
        serviceFuture.foreach(_.release())
      } finally {
        super.close()
      }
    }
  }
}
