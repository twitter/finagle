package com.twitter.finagle.kestrel

import scala.collection.JavaConversions._
import _root_.java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.{
  Future, Duration, Time,
  Return, Throw, Promise,
  Timer, NullTimer}
import com.twitter.conversions.time._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.{ServiceFactory, Service}
import com.twitter.concurrent.{Offer, Broker}

object ReadClosedException extends Exception
object OutOfRetriesException extends Exception

// TODO(Raghavendra Prabhu): Move ReadHandle and ReadMessage to util-core.

/**
 * A message that has been read: consists of the message itself, and
 * an offer to acknowledge.
 */
case class ReadMessage(bytes: ChannelBuffer, ack: Offer[Unit])

/**
 * An ongoing transactional read (from {{read}}).
 *
 * A common usage pattern is to attach asynchronous handlers to `messages` and `error`
 * by invoking `Offer.foreach` on them. For example:
 * {{{
 * val readHandle: ReadHandle = ...
 * readHandle.messages foreach { msg =>
 *   try {
 *     System.out.println(msg.bytes.toString("UTF-8"))
 *   } finally {
 *     msg.ack() // if we don't do this, no more msgs will come to us
 *   }
 * }
 * readHandle.error foreach { System.error.println("zomg! got an error " + _.getMessage) }
 * }}}
 */
trait ReadHandle {
  /**
   * An offer to synchronize on the next message.  A new message is
   * available only when the previous one has been acknowledged
   * (through {{ReadMessage.ack()}})
   */
  val messages: Offer[ReadMessage]

  /**
   * Indicates an error in the read.
   */
  val error: Offer[Throwable]

  /**
   * Closes the read.  Closes are signaled as an error with
   * {{ReadClosedException}} when the close has completed.
   */
  def close()

  /**
   * A copy of this {{ReadHandle}} that is buffered: it will make
   * available {{howmany}} messages at once, proactively acknowledging
   * them.  This allows a consumer to process {{howmany}} items in
   * parallel from one handle.
   */
  def buffered(howmany: Int): ReadHandle = {
    val out = new Broker[ReadMessage]
    val ack = new Broker[Unit]
    val closeReq = new Broker[Unit]
    def loop(nwait: Int, closed: Boolean) {
      // we're done if we're closed, and
      // we're not awaiting any acks.
      if (closed && nwait == 0) {
        close()
        return
      }

      Offer.select(
        if (nwait < howmany && !closed) {
          messages { m =>
            m.ack.sync()
            out ! m.copy(ack = ack.send(()))
            loop(nwait + 1, closed)
          }
        } else {
          Offer.never
        },

        ack.recv { _ =>
          loop(nwait - 1, closed)
        },

        closeReq.recv { _ =>
          loop(nwait, true)
        }
      )
    }

    loop(0, false)

    val underlying = this
    new ReadHandle {
      val messages = out.recv
      // todo: should errors be sequenced
      // with respect to messages here, or
      // just (as is now) be propagated
      // immediately.
      val error = underlying.error
      def close() = closeReq ! ()
    }
  }
}

object ReadHandle {
  // A convenience constructor using an offer for closing.
  def apply(
    _messages: Offer[ReadMessage],
    _error: Offer[Throwable],
    closeOf: Offer[Unit]
  ): ReadHandle = new ReadHandle {
    val messages = _messages
    val error = _error
    def close() = closeOf.sync()
  }

  /**
   * Provide a merged ReadHandle, combining the messages & errors of
   * the given underlying handles.  Closing this handle will close all
   * of the underlying ones.
   */
  def merged(handles: Seq[ReadHandle]): ReadHandle = new ReadHandle {
    val messages = Offer.choose(handles map { _.messages } toSeq:_*)
    val error = Offer.choose(handles map { _.error } toSeq:_*)
    def close() = handles foreach { _.close() }
  }

  /**
   * A java-friendly interface to {{merged}}
   */
  def merged(handles: _root_.java.util.Iterator[ReadHandle]): ReadHandle =
    merged(handles.toSeq)
}

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
   * if it isn't dequeued in time).
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
   * Read indefinitely from the given queue with transactions.  Note
   * that {{read}} will reserve a connection for the duration of the
   * read.  Note that this does no buffering: we await acknowledment
   * (through synchronizing on ReadMessage.ack) before acknowledging
   * that message to the kestrel server & reading the next one.
   *
   * @return A read handle.
   */
  def read(queueName: String): ReadHandle

  /**
   * Read from a queue reliably: retry streaming reads on failure
   * (which may indeed be backed by multiple kestrel hosts).  This
   * presents to the user a virtual "reliable" stream of messages, and
   * errors are transparent.
   *
   * @param queueName the queue to read from
   * @param timer a timer used to delay retries
   * @param retryBackoffs a (possibly infinite) stream of durations
   * comprising a backoff policy
   *
   * Note: the use of call-by-name for the stream is in order to
   * ensure that we do not suffer a space leak for infinite retries.
   */
  def readReliably(
    queueName: String,
    timer: Timer,
    retryBackoffs: => Stream[Duration]
  ): ReadHandle = {
    val error = new Broker[Throwable]
    val messages = new Broker[ReadMessage]
    val close = new Broker[Unit]

    def loop(handle: ReadHandle, backoffs: Stream[Duration]) {
      Offer.select(
        // proxy messages
        handle.messages { m =>
          messages ! m
          // a succesful read always resets the backoffs
          loop(handle, retryBackoffs)
        },

        // retry on error
        handle.error { t =>
          backoffs match {
            case delay #:: rest =>
              timer.schedule(delay.fromNow) { loop(read(queueName), rest) }
            case _ =>
              error ! OutOfRetriesException
          }
        },

        // proxy the close, and close our reliable channel
        close.recv { _=>
          handle.close()
          error ! ReadClosedException
        }
      )
    }

    loop(read(queueName), retryBackoffs)

    ReadHandle(messages.recv, error.recv, close.send(()))
  }

  /**
   * {{readReliably}} with infinite, 0-second backoff retries.
   */
  def readReliably(queueName: String): ReadHandle =
    readReliably(queueName, new NullTimer, Stream.continually(0.seconds))

  /*
   * Write indefinitely to the given queue.  The given offer is
   * synchronized on indefinitely, writing the items as they become
   * available.  Unlike {{read}}, {{write}} does not reserve a
   * connection.
   *
   * @return a Future indicating client failure.
   */
  def write(queueName: String, offer: Offer[ChannelBuffer]): Future[Throwable]

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
    underlying.toService(Flush(queueName))
  }

  def delete(queueName: String) = {
    underlying.toService(Delete(queueName))
  }

  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch) = {
    underlying.toService(Set(queueName, expiry, value))
  }

  def get(queueName: String, waitUpTo: Duration = 0.seconds) = {
    underlying.toService(Get(queueName, Some(waitUpTo))) map {
      case Values(Seq()) => None
      case Values(Seq(Value(key, value))) => Some(value)
      case _ => throw new IllegalArgumentException
    }
  }

  // note: this implementation uses "GET" requests, not "MONITOR",
  // so it will incur many roundtrips on quiet queues.
  def read(queueName: String): ReadHandle = {
    val error = new Broker[Throwable]  // this is sort of like a latch …
    val messages = new Broker[ReadMessage]  // todo: buffer?
    val close = new Broker[Unit]

    val open = Open(queueName, Some(Duration.Top))
    val closeAndOpen = CloseAndOpen(queueName, Some(Duration.Top))
    val abort = Abort(queueName)

    def recv(service: Service[Command, Response], command: GetCommand) {
      val reply = service(command)
      Offer.select(
        reply.toOffer {
          case Return(Values(Seq(Value(_, item)))) =>
            val ack = new Broker[Unit]
            messages ! ReadMessage(item, ack.send(()))

            Offer.select(
              ack.recv { _ => recv(service, closeAndOpen) },
              close.recv { t => service.close(); error ! ReadClosedException }
            )

          case Return(Values(Seq())) =>
            recv(service, open)

          case Return(_) =>
            service.close()
            error ! new IllegalArgumentException("invalid reply from kestrel")

          case Throw(t) =>
            service.close()
            error ! t
        },

        close.recv { _ =>
          service.close()
          reply.raise(ReadClosedException)
          error ! ReadClosedException
        }
      )
    }

    underlying() onSuccess { recv(_, open) } onFailure { error ! _ }

    ReadHandle(messages.recv, error.recv, close.send(()))
  }

  def write(queueName: String, offer: Offer[ChannelBuffer]): Future[Throwable] = {
    val closed = new Promise[Throwable]
    write(queueName, offer, closed)
    closed
  }

  private[this] def write(
    queueName: String,
    offer: Offer[ChannelBuffer],
    closed: Promise[Throwable]
  ) {
    offer.sync() foreach { item =>
      set(queueName, item).unit onSuccess { _ =>
        write(queueName, offer)
      } onFailure { t =>
        closed() = Return(t)
      }
    }
  }

  def close() {
    underlying.close()
  }
}
