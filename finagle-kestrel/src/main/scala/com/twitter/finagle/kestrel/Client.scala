package com.twitter.finagle.kestrel

import _root_.java.util.concurrent.atomic.AtomicBoolean
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
      .hostConnectionLimit(1)
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
   * @param  expiry  how long the item is valid for (Kestrel will delete the item
   * if it isn't dequeued in time.
   */
  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch): Future[Response]

  /**
   * Dequeue an item.
   *
   * @param  waitUpTo  if the queue is empty, indicate to the Kestrel server how
   * long to block the operation, waiting for something to arrive, before returning None
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
   * Get a Channel for the given queue, for reading. Messages begin dequeueing
   * from the Server when the first Observer responds, and pauses when all
   * Observers have disposed. Messages are acknowledged (closed) on the remote
   * server when all observers have successfully completed their write Future.
   * If any observer's write Future errors, the Channel is closed and the
   * item is rolled-back (aborted) on the remote server.
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

  /**
   * Close any consume resources such as TCP Connections. This should will not
   * release resources created by the from() and to() methods; it is the
   * responsibility of the caller to release those resources directly.
   */
  def close()
}

/**
 * A Client representing a single TCP connection to a single server.
 *
 * @param underlying  a ServiceFactory[Command, Response].
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
    val result = new ChannelSource[ChannelBuffer]
    var isRunning: AtomicBoolean = null
    result.numObservers.respond { i: Int =>
      i match {
        case 0 =>
          isRunning.set(false)
        case 1 =>
          isRunning = new AtomicBoolean(true)
          underlying.make() onSuccess { service =>
            receive(isRunning, service, queueName, result, waitUpTo, collection.Set(Open()))
          } onFailure { e =>
            result.close()
          }
        case _ =>
      }
      Future.Done
    }
    result
  }

  def to(queueName: String): ChannelSource[ChannelBuffer] = {
    val to = new ChannelSource[ChannelBuffer]
    to.respond { item =>
      set(queueName, item).unit onFailure { _ =>
        to.close()
      }
    }
    to
  }

  private[this] def receive(
    isRunning: AtomicBoolean,
    service: Service[Command, Response],
    queueName: String,
    channel: ChannelSource[ChannelBuffer],
    waitUpTo: Duration,
    options: collection.Set[GetOption])
  {
    // serialize() because of the check(isRunning)-then-act(send) idiom.
    channel.serialized {
      if (isRunning.get) {
        val request = Get(queueName, collection.Set(Timeout(waitUpTo)) ++ options)
        if (service.isAvailable)
        service(request) onSuccess {
          case Values(Seq(Value(key, item))) =>
            try {
              Future.join(channel.send(item)) onSuccess { _ =>
                receive(isRunning, service, queueName, channel, waitUpTo, collection.Set(Close(), Open()))
              } onFailure { e =>
                service(Get(queueName, collection.Set(Abort())))
                channel.close()
              }
            }
          case Values(Seq()) =>
            receive(isRunning, service, queueName, channel, waitUpTo, collection.Set(Open()))
          case _ => throw new IllegalArgumentException
        } onFailure { e =>
          e.printStackTrace()
          channel.close()
        }
      } else {
        service.release()
      }
    }
  }

  private[this] class ChannelSourceWithService(serviceFuture: Future[Service[Command, Response]]) extends ChannelSource[ChannelBuffer] {
    serviceFuture handle {
      case e =>
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

  def close() {
    underlying.close()
  }
}
