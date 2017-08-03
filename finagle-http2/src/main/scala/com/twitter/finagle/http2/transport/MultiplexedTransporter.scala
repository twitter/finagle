package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{Status, StreamClosedException, FailureFlags, Stack, Failure}
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.{Logger, HasLogLevel, Level}
import com.twitter.util.{Future, Time, Promise, Return, Throw, Try, Closable}
import io.netty.handler.codec.http.{HttpObject, LastHttpContent}
import io.netty.handler.codec.http2.Http2Error
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.HashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
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
  addr: SocketAddress,
  params: Stack.Params
) extends (() => Try[Transport[HttpObject, HttpObject]])
    with Closable { parent =>

  import MultiplexedTransporter._

  private[this] val log = Logger.get(getClass.getName)

  // A map of streamIds -> ChildTransport
  private[this] val children = new HashMap[Int, ChildTransport]()
  private[this] val id = new AtomicInteger(1)

  // This state as well as operations that start or stop streams and goaways are synchronized on
  // this (parent).
  private[this] var dead = false

  // exposed for testing
  private[http2] def numChildren: Int = synchronized { children.size() }
  private[http2] def setStreamId(num: Int): Unit = id.set(num)

  private[this] val FailureDetector.Param(detectorConfig) = params[FailureDetector.Param]
  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val pingPromise = new AtomicReference[Promise[Unit]]()

  private[this] def ping(): Future[Unit] = {
    val done = new Promise[Unit]
    if (pingPromise.compareAndSet(null, done)) {
      underlying.write(Ping).before(done)
    } else {
      FuturePingNack
    }
  }

  private[this] val detector =
    FailureDetector(detectorConfig, ping, statsReceiver.scope("failuredetector"))

  private[this] def handleGoaway(obj: HttpObject, lastStreamId: Int): Unit = parent.synchronized {
    dead = true
    children.values.asScala.foreach { child =>
      if (child.curId > lastStreamId) {
        child.close()
      }
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
    case Message(msg, streamId) =>
      val c = parent.synchronized { children.get(streamId) }
      if (c != null) c.offer(msg)
      else {
        val lastId = id.get()
        if (log.isLoggable(Level.DEBUG))
          log.debug(
            s"Got message for nonexistent stream $streamId. Next client " +
              s"stream id=${lastId}. msg=$msg"
          )

        if (streamId < id.get()) {
          underlying.write(Rst(streamId, Http2Error.STREAM_CLOSED.code))
        } else {
          parent.synchronized {
            dead = true
            children.values.asScala.foreach(_.close())
          }
          underlying.write(
            GoAway(
              LastHttpContent.EMPTY_LAST_CONTENT, // TODO: Properly (and carefully) utilize the
              lastId, //       debugData section of GoAway
              Http2Error.PROTOCOL_ERROR.code
            )
          )
          close()
        }
      }

    case GoAway(msg, lastStreamId, _) =>
      handleGoaway(msg, lastStreamId)

    case Rst(streamId, errorCode) =>
      val error =
        if (errorCode == Http2Error.REFUSED_STREAM.code) Failure.RetryableNackFailure
        else if (errorCode == Http2Error.ENHANCE_YOUR_CALM.code) Failure.NonRetryableNackFailure
        else new StreamClosedException(addr, streamId.toString)

      val c = parent.synchronized { children.get(streamId) }
      if (c != null) c.closeWith(error)
      else {
        if (log.isLoggable(Level.DEBUG))
          log.debug(s"Got RST for nonexistent stream: $streamId, code: $errorCode")
      }
    // According to spec, an endpoint should not send another RST upon receipt
    // of an RST for an absent stream ID as this could cause a loop.

    case Ping =>
      pingPromise.get.setDone()

    case rep =>
      if (log.isLoggable(Level.DEBUG)) {
        val name = rep.getClass.getName
        log.debug(
          s"we only support Message, GoAway, Rst right now but got $name. "
            + s"$name#toString returns: $rep"
        )
      }
  }

  private[this] val handleRead: Try[StreamMessage] => Future[Unit] = {
    case Return(msg) =>
      handleSuccessfulRead(msg)
      Future.Done

    case t @ Throw(e) =>
      parent.synchronized {
        children.values.asScala.foreach { c =>
          c.closeWith(e)
        }
        parent.close()
      }
      Future.const(t.cast[Unit])
  }

  // we should stop when we fail
  private[this] def loop(): Future[Unit] =
    underlying
      .read()
      .transform(handleRead)
      .before(loop())

  loop()

  /**
   * Provides a transport that knows we've already sent the first request, so we
   * just need a response to advance to the next stream.
   */
  def first(): Transport[HttpObject, HttpObject] = parent.synchronized {
    val ct = new ChildTransport()
    ct.newStream()
    ct.state = Active(finishedWriting = true, finishedReading = false)
    ct
  }

  def apply(): Try[Transport[HttpObject, HttpObject]] = parent.synchronized {
    if (dead) Throw(new DeadConnectionException(addr, FailureFlags.Retryable))
    else {
      Return(new ChildTransport())
    }
  }

  def onClose: Future[Throwable] = underlying.onClose

  def close(deadline: Time): Future[Unit] = underlying.close(deadline)

  def status: Status = detector.status

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

    @volatile private[this] var _curId = 0

    // Exposed for testing
    private[http2] def curId: Int = _curId

    private[this] val queue: AsyncQueue[HttpObject] =
      new AsyncQueue[HttpObject](ChildMaxPendingOffers)

    @volatile private[MultiplexedTransporter] var state: ChildState = Idle

    private[MultiplexedTransporter] def newStream(): Unit = parent.synchronized {
      state match {
        case Idle =>
          val newId = id.getAndAdd(2)
          if (newId < 0) {
            closeWith(new StreamIdOverflowException(addr))
          } else if (newId % 2 != 1) {
            closeWith(new IllegalStreamIdException(addr, newId))
          } else {
            // If the queue is not empty, this means a new stream is attempting to be created before
            // all of the messages from the previous stream have been consumed.
            val queueSize = queue.size
            if (queueSize != 0) {
              val head = queue.poll().poll match {
                case Some(thing) => thing.toString
                case None => "(no head?)"
              }
              badState(s"Queue was not empty (size=$queueSize) in newStream. Head: $head")
            } else {
              state = Active(finishedReading = false, finishedWriting = false)
              children.put(newId, child)
              children.remove(_curId)
              _curId = newId
            }

            // TODO: It is also a problem if someone is already polling on the queue in an idle
            //       state. This represents a premature read on a child that is not yet associated
            //       with a stream ID.
          }

        case _ =>
          badState(s"newStream in state: ${state.getClass.getName}, parent dead? ${parent.dead}")
      }
    }

    private[this] def badState(msg: String) = {
      log.error(s"Child ${_curId} bad state: $msg")
      val exn = new BadChildStateException(msg, curId)
      closeWith(exn)
      Future.exception(exn)
    }

    // must be synchronized externally
    private[this] def checkFinished() = {
      // We need to make sure that we are both finished writing,
      // receiving stream messages, _and_ have consumed all the stream
      // messages before resetting the state to Idle.
      state match {
        case a: Active if a.finished && queue.size == 0 =>
          if (parent.dead) close()
          else state = Idle

        case _ => // nop
      }
    }

    private[this] val postRead: Try[HttpObject] => Unit = {
      case Return(_) => // nop

      case Throw(e) =>
        closeWith(e)
    }

    // must be synchronized externally
    private[this] def writeAndCheck(obj: HttpObject): Future[Unit] = {
      state match {
        case a: Active =>
          if (obj.isInstanceOf[LastHttpContent]) {
            a.finishedWriting = true
            checkFinished()
          }

          underlying
            .write(Message(obj, curId))
            .onFailure { e: Throwable =>
              closeWith(e)
            }

        case _ =>
          _onClose.unit
      }
    }

    def write(obj: HttpObject): Future[Unit] = parent.synchronized {
      state match {
        case Idle =>
          // parent.dead is only examined when starting or finishing a stream
          if (!parent.dead) {
            newStream()
            writeAndCheck(obj)
          } else {
            log.warning("Write to child with dead parent")
            close()
          }

        case a: Active if a.finishedWriting =>
          badState(s"Write after finished writing: $obj").unit

        case a: Active =>
          writeAndCheck(obj)

        case Dead =>
          badState(s"Write to dead child: $obj").unit
      }
    }

    private[this] val closeOnInterrupt: PartialFunction[Throwable, Unit] = {
      case _: Throwable =>
        close()
    }

    def read(): Future[HttpObject] = {
      state match {
        case a: Active =>
          val result = queue.poll().map { pollResult =>
            // See if we're finished reading before delivering the result.
            parent.synchronized { checkFinished() }
            pollResult
          }
          result.respond(postRead)

          val p = Promise[HttpObject]
          result.proxyTo(p)

          p.setInterruptHandler(closeOnInterrupt)
          p

        case Dead =>
          queue.poll()

        case Idle =>
          badState("Read from idle child")
      }
    }

    // This should NOT be called in a synchronized block
    private[MultiplexedTransporter] def offer(obj: HttpObject): Unit = {
      val shouldOffer = parent.synchronized {
        state match {
          case a: Active if !a.finishedReading =>
            if (obj.isInstanceOf[LastHttpContent]) {
              a.finishedReading = true
            }
            true

          case _: Active =>
            // Technically, this condition is a protocol or stream error depending on
            // whether the stream is fully or only half closed, but we are going to be
            // lenient right now as our server implementation may be doing this and we
            // don't want to clobber any other active streams until the server is
            // behaving. See https://tools.ietf.org/html/rfc7540#section-5.1
            log.warning(s"Received message on inbound-closed stream: ($obj)")
            false

          case Idle =>
            badState(s"Offered message to idle child: $obj")
            false

          case Dead =>
            badState(s"Offered message to dead child: $obj")
            false
        }
      }
      // NB: Cannot offer inside of a synchronized block. This can cause deadlocks
      //     There is still a possibility of a race condition here, but that can
      //     be addressed by switching to an executor model which ensures sequential
      //     but lock free executions of critical sections.
      if (shouldOffer && !queue.offer(obj)) {
        badState(s"Failed to enqueue message. Queue size: ${queue.size}, msg: $obj")
      }
    }

    def status: Status = parent.synchronized {
      state match {
        case _: Active => Status.Open
        case Dead => Status.Closed
        case Idle => parent.status
      }
    }

    def onClose: Future[Throwable] = _onClose.or(underlying.onClose)

    def localAddress: SocketAddress = underlying.localAddress

    def remoteAddress: SocketAddress = underlying.remoteAddress

    def peerCertificate: Option[Certificate] = underlying.peerCertificate

    def close(deadline: Time): Future[Unit] = {
      closeWith(new StreamClosedException(addr, _curId.toString), deadline)
    }

    private[http2] def closeWith(exn: Throwable, deadline: Time = Time.Bottom): Future[Unit] = {
      parent.synchronized {
        state match {
          case a: Active if !a.finished =>
            underlying
              .write(Rst(_curId, Http2Error.CANCEL.code))
              .by(deadline)(DefaultTimer) // TODO: Get Timer from stack params
          case _ =>
        }

        state = Dead
        children.remove(curId)
      }

      _onClose.updateIfEmpty(Return(exn))
      queue.fail(exn, discard = false)
      _onClose.unit
    }
  }
}

private[http2] object MultiplexedTransporter {
  val FuturePingNack: Future[Nothing] =
    Future.exception(Failure("A ping is already outstanding on this session."))

  val ChildMaxPendingOffers = 1000

  sealed trait ChildState

  /**
   * Child is between requests and requires a new stream ID to continue
   */
  object Idle extends ChildState

  /**
   * Child represents a stream with active reading/writing
   */
  case class Active(var finishedReading: Boolean, var finishedWriting: Boolean) extends ChildState {
    def finished = finishedWriting && finishedReading
  }

  /**
   * Child is closed/dead and cannot be used again
   */
  object Dead extends ChildState

  class BadChildStateException(
    msg: String,
    id: Int,
    private[finagle] val flags: Long = FailureFlags.NonRetryable
  ) extends Exception(s"Child $id in bad state: $msg")
      with FailureFlags[BadChildStateException] {

    protected def copyWithFlags(newFlags: Long): BadChildStateException =
      new BadChildStateException(msg, id, newFlags)
  }

  class DeadConnectionException(addr: SocketAddress, private[finagle] val flags: Long)
      extends Exception(s"assigned an already dead connection to address $addr")
      with FailureFlags[DeadConnectionException] {

    protected def copyWithFlags(newFlags: Long): DeadConnectionException =
      new DeadConnectionException(addr, newFlags)
  }

  class StreamIdOverflowException(
    addr: SocketAddress,
    private[finagle] val flags: Long = FailureFlags.Retryable
  ) extends Exception(s"ran out of stream ids for address $addr")
      with FailureFlags[StreamIdOverflowException]
      with HasLogLevel {
    def logLevel: Level = Level.INFO // this is normal behavior, so we should log gently
    protected def copyWithFlags(flags: Long): StreamIdOverflowException =
      new StreamIdOverflowException(addr, flags)
  }

  class IllegalStreamIdException(
    addr: SocketAddress,
    id: Int,
    private[finagle] val flags: Long = FailureFlags.Retryable
  ) extends Exception(
        s"Found an invalid stream id $id on address $addr. "
          + "The id was even, but client initiated stream ids must be odd."
      )
      with FailureFlags[IllegalStreamIdException]
      with HasLogLevel {
    def logLevel: Level = Level.ERROR
    protected def copyWithFlags(flags: Long): IllegalStreamIdException =
      new IllegalStreamIdException(addr, id, flags)
  }
}
