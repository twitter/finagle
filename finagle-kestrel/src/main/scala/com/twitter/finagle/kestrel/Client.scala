package com.twitter.finagle.kestrel

import _root_.java.util.concurrent.atomic.AtomicBoolean
import _root_.java.util.logging.{Logger, Level}
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
      .codec(Kestrel())
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
protected[kestrel] class ConnectedClient(underlying: ServiceFactory[Command, Response])
  extends Client
{
  private[this] val log = Logger.getLogger(getClass.getName)

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
    underlying.service(Get(queueName, Some(waitUpTo))) map {
      case Values(Seq()) => None
      case Values(Seq(Value(key, value))) => Some(value)
      case _ => throw new IllegalArgumentException
    }
  }

  def from(queueName: String, waitUpTo: Duration = 10.seconds): Channel[ChannelBuffer] = {
    val result = new ChannelSource[ChannelBuffer]
    val isRunning = new AtomicBoolean(false)
    result.numObservers respond { i: Int =>
      i match {
        case 0 =>
          isRunning.set(false)
        case 1 =>
          // only start receiving if we weren't already running
          if (!isRunning.getAndSet(true)) {
            underlying.make() onSuccess { service =>
              receive(isRunning, service, Open(queueName, Some(waitUpTo)), result)
            } onFailure { t =>
              log.log(Level.WARNING, "Could not make service", t)
              result.close()
            }
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
    command: GetCommand,
    channel: ChannelSource[ChannelBuffer])
  {
    def receiveAgain(command: GetCommand) {
      receive(isRunning, service, command, channel)
    }

    def cleanup() {
      channel.close()
      service.release()
    }

    // serialize() because of the check(isRunning)-then-act(send) idiom.
    channel.serialized {
      if (isRunning.get && service.isAvailable) {
        service(command) onSuccess {
          case Values(Seq(Value(key, item))) =>
            try {
              Future.join(channel.send(item)) onSuccess { _ =>
                receiveAgain(CloseAndOpen(command.queueName, command.timeout))
              } onFailure { t =>
                // abort if not all observers ack the send
                service(Abort(command.queueName)) ensure { cleanup() }
              }
            }

          case Values(Seq()) =>
            receiveAgain(Open(command.queueName, command.timeout))

          case _ =>
            throw new IllegalArgumentException

        } onFailure { t =>
          log.log(Level.WARNING, "service produced exception", t)
          cleanup()
        }
      } else {
        cleanup()
      }
    }
  }

  private[this] class ChannelSourceWithService(serviceFuture: Future[Service[Command, Response]])
    extends ChannelSource[ChannelBuffer]
  {
    private[this] val log = Logger.getLogger(getClass.getName)

    serviceFuture handle { case t =>
      log.log(Level.WARNING, "service produced exception", t)
      this.close()
    }

    override def close() {
      try {
        serviceFuture.foreach { _.release() }
      } finally {
        super.close()
      }
    }
  }

  def close() {
    underlying.close()
  }
}
