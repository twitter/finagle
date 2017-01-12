package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{Status, StreamClosedException, FailureFlags}
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.{Logger, HasLogLevel, Level}
import com.twitter.util.{Future, Time, Promise, Return, Throw, Try}
import io.netty.handler.codec.http.{HttpObject, LastHttpContent}
import io.netty.handler.codec.http2.Http2Error
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

/**
 * A factory for making a transport which represents an http2 stream.
 *
 * After a stream finishes, the transport represents the next stream id.
 *
 * Since each transport only represents a single stream at a time, one of these
 * transports cannot be multiplexed-instead, the ChildTransport is the
 * unit of multiplexing, so if you want to increase concurrency, you should
 * create a new Transport.
 */
private[http2] class MultiplexedTransporter(
    underlying: Transport[StreamMessage, StreamMessage],
    addr: SocketAddress)
  extends (() => Try[Transport[HttpObject, HttpObject]]) { self =>

  import MultiplexedTransporter._

  private[this] val log = Logger.get(getClass.getName)
  private[this] val queues: ConcurrentHashMap[Int, AsyncQueue[HttpObject]] =
    new ConcurrentHashMap[Int, AsyncQueue[HttpObject]]()
  private[this] val id = new AtomicInteger(1)

  // synchronized by this
  private[this] var curStreams = 0
  private[this] var dead = false

  // exposed for testing
  private[http2] def setStreamId(num: Int): Unit = id.set(num)

  // TODO: the correct goaway behavior is to wait for requests lower than
  // lastStreamId and assume everything higher will never see a response.
  // instead, we fail everything else in-flight.
  private[this] def failEverything(response: HttpObject): Unit = {
    queues.values.asScala.foreach { q =>
      q.offer(response)
    }
    queues.clear()
  }

  private[this] def tryToInitializeQueue(num: Int): AsyncQueue[HttpObject] = {
    val q = queues.get(num)
    if (q != null) q
    else {
      val newQ = new AsyncQueue[HttpObject]()
      if (queues.putIfAbsent(num, newQ) == null) newQ else tryToInitializeQueue(num)
    }
  }

  // this should be in-order because the underlying queue is enqueued to by a
  // single thread.  however, this is fragile and should be replaced.  one
  // place where this might have trouble is within a single scheduled block,
  // with an adversarial scheduler.  imagine that we're already in a scheduled
  // block when we enqueue two items from the same stream.  they're both
  // dequeued, and on dequeue they're scheduled to be re-enqueued on the
  // stream-specific queue.  however, they're scheduled in the order in which
  // they were enqueued, but not necessarily executed in that order.  with an
  // adversarial scheduler, they might be executed in opposite order.  this is
  // not hypothetical--promises that have been satisfied already (for example
  // if the response is read before the stream calls read) may be scheduled in
  // a different order, or on a different thread. it's unlikely, but racy.

  private[this] def handleSuccessfulRead(sm: StreamMessage): Unit = sm match {
    case Message(msg, streamId) => tryToInitializeQueue(streamId).offer(msg)
    case GoAway(response) => failEverything(response)
    case Rst(streamId, _) =>
      // TODO clean up handling for various rsts
      tryToInitializeQueue(streamId).fail(new StreamClosedException(addr, streamId.toString))
    case rep =>
      val name = rep.getClass.getName
      log.error(s"we only support Message, GoAway, Rst right now but got $name. "
        + s"$name#toString returns: $rep")
  }

  private[this] val handleRead: Try[StreamMessage] => Future[Unit] = {
    case Return(msg) =>
      handleSuccessfulRead(msg)
      Future.Done
    case t@Throw(e) =>
      queues.asScala.toMap.foreach { case (_, q) =>
        q.fail(e)
      }
      queues.clear()
      Future.const(t.cast[Unit])
  }

  // we should stop when we fail
  private[this] def loop(): Future[Unit] = underlying.read()
    .transform(handleRead)
    .before(loop())

  // TODO: clean this up when we figure out the semantics of closing a connection
  loop()

  /**
   * Provides a transport that knows we've already sent the first request, so we
   * just need a response to advance to the next stream.
   */
  def first(): Transport[HttpObject, HttpObject] = {
    val ct = createChildTransport()

    ct.synchronized {
      ct.finishedWriting = true
      ct.startedStream = true
    }
    ct
  }

  private[this] def createChildTransport(): ChildTransport = self.synchronized {
    val ct = new ChildTransport()
    curStreams += 1
    ct.onClose.ensure {
      self.synchronized {
        curStreams -= 1
        if (curStreams == 0) {
          dead = true
          underlying.close()
        }
      }
    }

    ct
  }

  def apply(): Try[Transport[HttpObject, HttpObject]] = self.synchronized {
    if (dead) Throw(new DeadConnectionException(addr, FailureFlags.Retryable))
    else Return(createChildTransport())
  }

  def onClose: Future[Throwable] = underlying.onClose

  /**
   * ChildTransport represents a single http/2 stream at a time.  Once the stream
   * has finished, the transport can be used again for a different stream.
   */
  // One note is that closing one of the transports closes the underlying
  // transport.  This doesn't play well with configurations that close idle
  // connections, since finagle doesn't know that these are streams, not
  // connections.  This can be improved by adding an extra layer between pooling
  // and dispatching, or making the distinction between streams and sessions
  // explicit.
  private[http2] class ChildTransport extends Transport[HttpObject, HttpObject] { child =>

    private[this] val _onClose: Promise[Throwable] = Promise[Throwable]()

    // all of these mutable fields are protected by synchronization on the
    // child class.

    // we're going to fix this immediately in incrementStream
    private[this] var curId = 0
    private[this] var queue: AsyncQueue[HttpObject] = null

    // we keep track of the next read explicitly here to ensure
    // that we notice when the stream is killed
    private[this] var nextRead: Future[HttpObject] = null
    incrementStream()

    private[MultiplexedTransporter] var finishedWriting = false
    private[this] var finishedReading = false
    private[this] var dead = false
    private[MultiplexedTransporter] var startedStream = false

    private[this] def handleCloses(f: Future[HttpObject]): Unit =
      f.onFailure { e =>
        _onClose.updateIfEmpty(Return(e))
      }

    def write(obj: HttpObject): Future[Unit] = child.synchronized {
      if (!dead) {
        startedStream = true
        val message = Message(obj, curId)

        if (obj.isInstanceOf[LastHttpContent]) {
          finishedWriting = true
          tryToIncrementStream()
        }
        underlying.write(message)
      } else {
        Future.exception(new StreamClosedException(addr, curId.toString))
      }
    }

    private[this] def tryToIncrementStream(): Unit = child.synchronized {
      if (finishedWriting && finishedReading) {
        finishedReading = false
        finishedWriting = false
        startedStream = false
        val result = queues.remove(curId)
        if (result == null) {
          log.error(s"Expected to remove stream id: $curId but it wasn't there")
        }
        incrementStream()
      }
    }

    // see http://httpwg.org/specs/rfc7540.html#StreamIdentifiers
    private[this] def incrementStream(): Unit = child.synchronized {
      curId = id.getAndAdd(2)

      // if you run out of integers, you must make a new connection
      if (curId < 0) {
        close()
        _onClose.updateIfEmpty(Return(new StreamIdOverflowException(addr)))
      } else if (curId % 2 != 1) { // stream ids initiated by the client must be odd
        val exn = new IllegalStreamIdException(addr, curId)
        close()
        _onClose.updateIfEmpty(Return(exn))
        log.error(exn, s"id $id was not odd, but client-side ids must be odd. this is a bug.")
      } else {
        queue = tryToInitializeQueue(curId)
        nextRead = queue.poll()
        handleCloses(nextRead)
      }
    }

    private[this] val checkStreamStatus: HttpObject => Unit = {
      case _: LastHttpContent =>
        child.synchronized {
          finishedReading = true
          tryToIncrementStream()
        }
      case _ =>
        // nop
    }

    private[this] val closeOnInterrupt: PartialFunction[Throwable, Unit] = { case _: Throwable =>
      close()
    }

    def read(): Future[HttpObject] = child.synchronized {
      val result = nextRead
      nextRead = queue.poll()
      handleCloses(nextRead)

      result.onSuccess(checkStreamStatus)

      val p = Promise[HttpObject]
      result.proxyTo(p)

      // we can improve interrupt behavior here
      p.setInterruptHandler(closeOnInterrupt)
      p
    }

    def status: Status = if (dead) Status.Closed else underlying.status

    def onClose: Future[Throwable] = _onClose.or(underlying.onClose)

    def localAddress: SocketAddress = underlying.localAddress

    def remoteAddress: SocketAddress = underlying.remoteAddress

    def peerCertificate: Option[Certificate] = underlying.peerCertificate

    def close(deadline: Time): Future[Unit] = {
      child.synchronized {
        if (!dead) {
          dead = true

          if (startedStream) {
            underlying.write(Rst(curId, Http2Error.CANCEL.code))
              .by(deadline)(DefaultTimer.twitter)
              .ensure {
                tryToInitializeQueue(curId).fail(new StreamClosedException(addr, curId.toString))
              }
          } else {
            tryToInitializeQueue(curId).fail(new StreamClosedException(addr, curId.toString))
          }
        }
      }
      _onClose.unit
    }
  }
}

private[http2] object MultiplexedTransporter {
  class DeadConnectionException(
      addr: SocketAddress,
      private[finagle] val flags: Long)
    extends Exception(s"assigned an already dead connection to address $addr")
    with FailureFlags[DeadConnectionException] {

    protected def copyWithFlags(newFlags: Long): DeadConnectionException =
      new DeadConnectionException(addr, newFlags)
  }

  class StreamIdOverflowException(
      addr: SocketAddress,
      private[finagle] val flags: Long = FailureFlags.Retryable)
    extends Exception(s"ran out of stream ids for address $addr")
    with FailureFlags[StreamIdOverflowException]
    with HasLogLevel {
    def logLevel: Level = Level.INFO // this is normal behavior, so we should log gently
    protected def copyWithFlags(flags: Long): StreamIdOverflowException =
      new StreamIdOverflowException(addr, flags)
  }

  class IllegalStreamIdException(
      addr: SocketAddress,
      id: Int,
      private[finagle] val flags: Long = FailureFlags.Retryable)
    extends Exception(s"Found an invalid stream id $id on address $addr. "
      + "The id was even, but client initiated stream ids must be odd.")
    with FailureFlags[IllegalStreamIdException]
    with HasLogLevel {
    def logLevel: Level = Level.ERROR
    protected def copyWithFlags(flags: Long): IllegalStreamIdException =
      new IllegalStreamIdException(addr, id, flags)
  }
}
