package com.twitter.finagle.kestrel

import scala.collection.JavaConversions._

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.conversions.time._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.{ServiceFactory, Service}
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.kestrel.net.lag.kestrel.thriftscala.Item
import com.twitter.finagle.kestrel.net.lag.kestrel.thriftscala.Kestrel.FinagledClient
import com.twitter.io.Buf
import com.twitter.util.{Command=>_, _}

/**
 * Indicates that a [[com.twitter.finagle.kestrel.ReadHandle]] has been closed.
 */
object ReadClosedException extends Exception

/**
 * Indicates that a [[com.twitter.finagle.kestrel.ReadHandle]] has exceeded its
 * retry budget.
 */
object OutOfRetriesException extends Exception

/**
 * A message that has been read: consists of the message itself, and
 * an offer to acknowledge.
 */
case class ReadMessage(
  bytes: Buf, ack: Offer[Unit], abort: Offer[Unit] = Offer.const(Unit)
)

/**
 * An ongoing transactional read (from {{read}}).
 *
 * A common usage pattern is to attach asynchronous handlers to `messages` and `error`
 * by invoking `Offer.foreach` on them. For example:
 * {{{
 * val readHandle: ReadHandle = ...
 * readHandle.messages.foreach { msg =>
 *   try {
 *     System.out.println(msg.bytes.toString("UTF-8"))
 *   } finally {
 *     msg.ack() // if we don't do this, no more msgs will come to us
 *   }
 * }
 * readHandle.error.foreach { System.error.println("zomg! got an error " + _.getMessage) }
 * }}}
 */
abstract class ReadHandle {
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
   * A Java-friendly API for the `ReadHandle() constructor`.
   */
  def fromOffers(
    messages: Offer[ReadMessage],
    error: Offer[Throwable],
    closeOf: Offer[Unit]
  ): ReadHandle = ReadHandle(messages, error, closeOf)


  /**
   * Provide a merged ReadHandle, combining the messages & errors of
   * the given underlying handles.  Closing this handle will close all
   * of the underlying ones.
   */
  def merged(handles: Seq[ReadHandle]): ReadHandle = new ReadHandle {
    val messages = Offer.choose(handles.map { _.messages }.toSeq:_*)
    val error = Offer.choose(handles.map { _.error }.toSeq:_*)
    def close() = handles.foreach { _.close() }
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
      .daemon(true)
      .buildFactory()
    apply(service)
  }

  /**
   * Create a client that uses Kestrel's thrift protocol.
   * @param raw the underlying thrift service factory for the client to use
   * @param txnAbortTimeout The duration after which an open transaction will be auto-aborted if not confirmed
   * @return A thrift kestrel client
   */
  // Due to type erasure this cannot be apply(raw: ServiceFactory[ThriftClientRequest, Array[Byte]])
  def makeThrift(raw: ServiceFactory[ThriftClientRequest, Array[Byte]], txnAbortTimeout: Duration): Client = {
    new ThriftConnectedClient(new FinagledClientFactory(raw), txnAbortTimeout)
  }

  def makeThrift(raw: ServiceFactory[ThriftClientRequest, Array[Byte]]): Client = {
    makeThrift(raw, Duration.Top)
  }

  private val nullTimer = new NullTimer
}

/**
 * A friendly Kestrel client Interface.
 */
trait Client {
  /**
   * Enqueue an item.
   *
   * @param  expiry  how long the item is valid for (Kestrel will delete the item
   * if it isn't dequeued in time).  Time.epoch (0) means the item will never
   * expire.
   */
  def set(queueName: String, value: Buf, expiry: Time = Time.epoch): Future[Response]

  /**
   * Dequeue an item.
   *
   * @param  waitUpTo  if the queue is empty, indicate to the Kestrel server how
   * long to block the operation, waiting for something to arrive, before
   * returning None.
   * 0.seconds (the default) means no waiting, as opposed to infinite wait.
   */
  def get(queueName: String, waitUpTo: Duration = 0.seconds): Future[Option[Buf]]

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
      Offer.prioritize(
        close.recv { _ =>
          handle.close()
          error ! ReadClosedException
        },
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
        }
      ).sync()
    }

    loop(read(queueName), retryBackoffs)

    ReadHandle(messages.recv, error.recv, close.send(()))
  }

  /**
   * {{readReliably}} with infinite, 0-second backoff retries.
   */
  def readReliably(queueName: String): ReadHandle =
    readReliably(queueName, Client.nullTimer, Stream.continually(0.seconds))

  /**
   * Write indefinitely to the given queue.  The given offer is
   * synchronized on indefinitely, writing the items as they become
   * available.  Unlike {{read}}, {{write}} does not reserve a
   * connection.
   *
   * @return a Future indicating client failure.
   */
  def write(queueName: String, offer: Offer[Buf]): Future[Throwable]

  /**
   * Close any consume resources such as TCP Connections. This should will not
   * release resources created by the from() and to() methods; it is the
   * responsibility of the caller to release those resources directly.
   */
  def close()
}

/**
 * Factory of command executors for [[com.twitter.finagle.kestrel.ClientBase]]
 * @tparam U the type used to execute commands in
 *           [[com.twitter.finagle.kestrel.ClientBase.read]]
 */
abstract protected[kestrel] class CommandExecutorFactory[U]
  extends Closable {
  def apply(): Future[U]
}

/**
 * Common base class for clients using different protocols
 * @param underlying the factory for creating command executors for a protocol
 * @tparam CommandExecutor the type that executes commands using some protocol
 * @tparam Reply the type of reply that {{CommandExecutor}} returns
 * @tparam ItemId the type used by {{CommandExecutor}} to identify returned
 *                items
 */
abstract protected[kestrel] class ClientBase[CommandExecutor <: Closable, Reply, ItemId](
    underlying: CommandExecutorFactory[CommandExecutor])
  extends Client
{
  /**
   * Read indefinitely from a underlying service
   * @param processResponse function to process a raw reply into:
   *   Return(Some()) successful read of a single item,
   *   Return(None) successful read of zero items, or
   *   Throw() invalid reply
   * @param openCommand the command to open a read
   * @param closeAndOpenCommand the command to ack and open a read
   * @param abortCommand the command to abort a read
   * @return a ReadHandle
   */
  // note: this implementation uses "GET" requests, not "MONITOR",
  // so it will incur many roundtrips on quiet queues.
  protected def read(
    processResponse: Reply => Try[Option[(Buf, ItemId)]],
    openCommand: CommandExecutor => Future[Reply],
    closeAndOpenCommand: ItemId => (CommandExecutor => Future[Reply]),
    abortCommand: ItemId => (CommandExecutor => Future[Reply])): ReadHandle = {

    val error = new Broker[Throwable]  // this is sort of like a latch...
    val messages = new Broker[ReadMessage]  // todo: buffer?
    val close = new Broker[Unit]
    val abort = new Broker[ItemId]

    def recv(service: CommandExecutor, command: CommandExecutor => Future[Reply]) {
      val reply = command(service)
      Offer.prioritize(
        close.recv { _ =>
          service.close()
          reply.raise(ReadClosedException)
          error ! ReadClosedException
        },
        reply.toOffer {
          case Return(r: Reply) =>
            processResponse(r) match {
              case Return(Some((data, id))) =>
                val ack = new Broker[ItemId]
                messages ! ReadMessage(data, ack.send(id), abort.send(id))
                Offer.prioritize(
                  close.recv { t => service.close(); error ! ReadClosedException },
                  ack.recv { id => recv(service, closeAndOpenCommand(id)) },
                  abort.recv { id => recv(service, abortCommand(id)) }
                ).sync()
              case Return(None) =>
                recv(service, openCommand)

              case Throw(t) =>
                service.close()
                error ! t
            }

          case Return(_) =>
            service.close()
            error ! new IllegalArgumentException("invalid reply from kestrel")

          case Throw(t) =>
            service.close()
            error ! t
        }
      ).sync()
    }

    underlying() respond {
      case Return(r) => recv(r, openCommand)
      case Throw(t) => error ! t
    }

    ReadHandle(messages.recv, error.recv, close.send(()))
  }

  def write(queueName: String, offer: Offer[Buf]): Future[Throwable] = {
    val closed = new Promise[Throwable]
    write(queueName, offer, closed)
    closed
  }

  private[this] def write(
    queueName: String,
    offer: Offer[Buf],
    closed: Promise[Throwable]
  ) {
    offer.sync().foreach { item =>
      set(queueName, item).unit respond {
        case Return(_) => write(queueName, offer)
        case Throw(t) => closed() = Return(t)
      }
    }
  }

  def close() {
    underlying.close()
  }
}

/**
 * Wrapper to map ServiceFactory[Command, Response] to abstraction used by
 * [[com.twitter.finagle.kestrel.ClientBase]]
 * @param underlying  the kestrel memcache service being wrapped
 */
private class MemcacheClientFactory(underlying: ServiceFactory[Command, Response])
  extends CommandExecutorFactory[Service[Command, Response]] {
  def apply() = underlying()
  def close(deadline: Time) = underlying.close(deadline)
}

object ConnectedClient {

  private val SomeTop = Some(Duration.Top)
}

/**
 * A Client representing a single TCP connection to a single server.
 *
 * @param underlying  a MemcacheClientFactory that wraps a
 *   ServiceFactory[Command, Response].
 */
protected[kestrel] class ConnectedClient(underlying: ServiceFactory[Command, Response])
  extends ClientBase[Service[Command, Response], Response, Unit](
    new MemcacheClientFactory(underlying)) {
  import ConnectedClient._

  def flush(queueName: String): Future[Response] =
    underlying.toService(Flush(Buf.Utf8(queueName)))

  def delete(queueName: String): Future[Response] =
    underlying.toService(Delete(Buf.Utf8(queueName)))

  def set(queueName: String, value: Buf, expiry: Time = Time.epoch): Future[Response] =
    underlying.toService(Set(Buf.Utf8(queueName), expiry, value))

  def get(queueName: String, waitUpTo: Duration = 0.seconds): Future[Option[Buf]] =
    underlying.toService(Get(Buf.Utf8(queueName), Some(waitUpTo))).map {
      case Values(Seq()) => None
      case Values(Seq(Value(key, value: Buf))) => Some(value)
      case _ => throw new IllegalArgumentException
    }

  private def MemCommand(command: Command)(service: Service[Command, Response]) =
    service(command)

  def read(queueName: String): ReadHandle = {
    val queueBuffer: Buf = Buf.Utf8(queueName)
    val open = MemCommand(Open(queueBuffer, SomeTop))_
    val closeAndOpen = MemCommand(CloseAndOpen(queueBuffer, SomeTop))_
    val abort = MemCommand(Abort(Buf.Utf8(queueName)))_

    read(
      (response: Response) =>
        response match {
          case Values(Seq(Value(_, item))) => Return(Some(item, ()))
          case Values(Seq()) => Return(None)
          case _ => Throw(new IllegalArgumentException("invalid reply from kestrel"))
        },
      open,
      (Unit) => closeAndOpen,
      (Unit) => abort)
  }
}

/**
 * Extends FinagledClient with Closable
 * @param service the underlying thrift service for FinagledClient
 */
protected[kestrel] class FinagledClosableClient(service: Service[ThriftClientRequest, Array[Byte]])
  extends FinagledClient(service)
  with Closable {
  def close(time: Time): Future[Unit] = service.close(time)
}

/**
 * Wrapper factory for creating FinagledClosableClients
 * @param underlying the underlying thrift service factory to wrap
 */
protected[kestrel] class FinagledClientFactory(
  underlying: ServiceFactory[ThriftClientRequest, Array[Byte]])
  extends CommandExecutorFactory[FinagledClosableClient] {
  def apply(): Future[FinagledClosableClient] =
    underlying().map { s => new FinagledClosableClient(s) }

  def close(deadline: Time): Future[Unit] = underlying.close(deadline)
}

/**
 * A Client representing a single TCP connection to a single server using thrift.
 *
 * @param underlying  a FinagledClientFactory that wraps a
 *   ServiceFactory[ThriftClientRequest, Array[Byte] ].
 */
protected[kestrel] class ThriftConnectedClient(underlying: FinagledClientFactory, txnAbortTimeout: Duration)
  extends ClientBase[FinagledClosableClient, Seq[Item], Long](underlying)
{
  private def safeLongToInt(l: Long): Int = {
    if (l > Int.MaxValue) Int.MaxValue
    else if (l < Int.MinValue) Int.MinValue
    else l.toInt
  }

  private def withClient[T](f: (FinagledClient) => Future[T]) =
    underlying() flatMap {
      client =>
        f(client) ensure  {
          client.service.close()
        }
      }

  def flush(queueName: String): Future[Response] =
    withClient[Values](client =>
      client.flushQueue(queueName).map {
        _ => Values(Nil)
      })

  def delete(queueName: String): Future[Response] =
    withClient[Response](client =>
      client.deleteQueue(queueName).map {
        _ => Deleted()
      })

  def set(queueName: String, value: Buf, expiry: Time = Time.epoch): Future[Response] = {
    val timeout = safeLongToInt(expiry.inMilliseconds)
    withClient[Response](client =>
      client.put(queueName, List(Buf.toByteBuffer(value)), timeout).map {
        _ => Stored()
      })
  }

  def get(queueName: String, waitUpTo: Duration = 0.seconds): Future[Option[Buf]] = {
    val waitUpToMsec = safeLongToInt(waitUpTo.inMilliseconds)
    withClient[Option[Buf]](client =>
      client.get(queueName, 1, waitUpToMsec).map {
        case Seq() => None
        case Seq(item: Item) => Some(Buf.ByteBuffer(item.data))
        case _ => throw new IllegalArgumentException
      })
  }

  private def openRead(queueName: String)(client: FinagledClosableClient): Future[Seq[Item]] =
    client.get(queueName, 1, Int.MaxValue, safeLongToInt(txnAbortTimeout.inMilliseconds.toLong))

  private def confirmAndOpenRead(queueName: String)
                                (id: Long)
                                (client: FinagledClosableClient): Future[Seq[Item]] =
    client.confirm(queueName, collection.Set(id)) flatMap {
      _ => openRead(queueName)(client)
    }

  private def abortReadCommand(queueName: String)
                              (id: Long)
                              (client: FinagledClosableClient): Future[Seq[Item]] =
    client.abort(queueName, collection.Set(id)).map {
      _ => collection.Seq[Item]()
    }

  def read(queueName: String): ReadHandle =
    read(
      (response: Seq[Item]) => response match {
        case Seq(Item(data, id)) => Return(Some(Buf.ByteBuffer(data), id))
        case Seq() => Return(None)
        case _ => Throw(new IllegalArgumentException("invalid reply from kestrel"))
      },
      openRead(queueName),
      confirmAndOpenRead(queueName),
      abortReadCommand(queueName))
}
