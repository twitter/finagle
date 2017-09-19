package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.{Transport, TransportContext, LegacyContext}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Status, StreamClosedException, FailureFlags, Stack, Failure}
import com.twitter.logging.{Logger, HasLogLevel, Level}
import com.twitter.util.{Future, Time, Promise, Return, Throw, Try, Closable}
import io.netty.handler.codec.http.{HttpObject, HttpRequest, LastHttpContent}
import io.netty.handler.codec.http2.Http2Error
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.ConcurrentHashMap
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
 *
 * ==Threading==
 *
 * MultiplexedTransporter requires the underlying transport to provide a serial
 * [[java.util.concurrent.Executor]]. That is, an `Executor` that guarantees
 * ordered execution of `Runnables` one at a time. This allows thread safety and
 * race condition avoidance without using synchronized blocks. This is desirable
 * because satisfying promises within synchronization blocks can cause
 * deadlocks.
 *
 * By convention, any methods prefixed with "handle" must be called from
 * within code running through the `Executor`. Finally, note that Locals are
 * not forwarded to the Runnables submitted to the executor, including the
 * Context.
 */
private[http2] class MultiplexedTransporter(
  underlying: Transport[StreamMessage, StreamMessage] {
    type Context = TransportContext with HasExecutor
  },
  addr: SocketAddress,
  params: Stack.Params
) extends (() => Try[Transport[HttpObject, HttpObject]])
    with Closable { parent =>
  import MultiplexedTransporter._

  private[this] val exec = underlying.context.executor
  private[this] val log = Logger.get(getClass.getName)

  // A map of streamIds -> ChildTransport
  private[this] val children = new ConcurrentHashMap[Int, ChildTransport]()
  private[this] val id = new AtomicInteger(1)

  // This state as well as operations that start or stop streams and goaways are synchronized on
  // this (parent).
  @volatile private[this] var dead = false

  // exposed for testing
  private[http2] def numChildren: Int = children.size
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

  private[this] def handleGoaway(obj: HttpObject, lastStreamId: Int): Unit = {
    dead = true
    children.values.asScala.foreach { child =>
      if (child.curId > lastStreamId) {
        child.handleCloseStream(s"the stream id (${child.curId}) was higher than the last stream" +
          s" id ($lastStreamId) on a GOAWAY")
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
      val c = children.get(streamId)
      if (c != null) c.handleOffer(msg)
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
          dead = true
          children.values.asScala.foreach(_.handleCloseStream(s"stream $streamId not found"))
          underlying.write(
            // TODO: Properly utilize the DEBUG_DATA section of GOAWAY
            GoAway(
              LastHttpContent.EMPTY_LAST_CONTENT,
              lastId,
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

      val c = children.get(streamId)
      if (c != null) c.handleCloseWith(error)
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

  private[this] val readLoop: Try[StreamMessage] => Unit = { result =>
    exec.execute(new Runnable {
      def run(): Unit = {
        result match {
          case Return(msg) =>
            handleSuccessfulRead(msg)
            loop()

          case Throw(e) =>
            children.values.asScala.foreach { c =>
              c.handleCloseWith(e)
            }
            parent.close()
        }
      }
    })
  }

  // we should stop when we fail
  private[this] def loop(): Unit = {
    underlying.read().respond(readLoop)
  }

  loop()

  /**
   * Provides a transport that knows we've already sent the first request, so we
   * just need a response to advance to the next stream.
   * These methods (first and apply) are not threadsafe and must be used
   * accordingly.
   */
  def first(): Transport[HttpObject, HttpObject] = {
    val ct = new ChildTransport()
    ct.handleNewStream()
    ct.handleState(Active(finishedWriting = true, finishedReading = false))
    ct
  }

  def apply(): Try[Transport[HttpObject, HttpObject]] = {
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
    type Context = TransportContext

    private[this] val _onClose: Promise[Throwable] = Promise[Throwable]()

    // Current stream ID
    @volatile private[this] var _curId = 0

    // Exposed for testing
    private[http2] def curId: Int = _curId

    private[this] val queue: AsyncQueue[HttpObject] =
      new AsyncQueue[HttpObject](ChildMaxPendingOffers)

    @volatile private[MultiplexedTransporter] var _state: ChildState = Idle

    private[MultiplexedTransporter] def handleState(newState: ChildState): Unit = {
      if (log.isLoggable(Level.DEBUG)) {
        log.debug(s"Stream $curId: ${_state} => ${newState}")
      }
      _state = newState
    }

    def state: ChildState = _state

    private[MultiplexedTransporter] def handleNewStream(): Unit = {
      state match {
        case Idle =>
          val newId = id.getAndAdd(2)
          if (newId < 0) {
            handleCloseWith(new StreamIdOverflowException(addr))
          } else if (newId % 2 != 1) {
            handleCloseWith(new IllegalStreamIdException(addr, newId))
          } else {
            // If the queue is not empty, this means a new stream is attempting to be created before
            // all of the messages from the previous stream have been consumed.
            val queueSize = queue.size
            if (queueSize != 0) {
              val head = queue.poll().poll match {
                case Some(thing) => thing.toString
                case None => "(no head?)"
              }
              handleBadState(
                s"Queue was not empty (size=$queueSize) when creating new stream. Head: $head"
              )
            } else {
              handleState(Active(finishedReading = false, finishedWriting = false))
              children.put(newId, child)
              children.remove(_curId)
              _curId = newId
            }

            // TODO: It is also a problem if someone is already polling on the queue in an idle
            //       state. This represents a premature read on a child that is not yet associated
            //       with a stream ID.
          }

        case _ =>
          handleBadState(
            s"newStream in state: ${state.getClass.getName}, parent dead? ${parent.dead}"
          )
      }
    }

    private[this] def handleBadState(msg: String): Future[Nothing] = {
      log.error(s"Child ${_curId} bad state: $msg")
      val exn = new BadChildStateException(msg, curId)
      handleCloseWith(exn)
      Future.exception(exn)
    }

    private[this] def handleCheckFinished() = {
      state match {
        case a: Active if a.finished && queue.size == 0 =>
          if (parent.dead) handleCloseStream(s"parent MultiplexedTransporter already dead")
          else handleState(Idle)

        case _ => // nop
      }
    }

    private[this] val postRead: Try[HttpObject] => Unit = {
      case Return(_) =>
        exec.execute(new Runnable { def run(): Unit = handleCheckFinished() })

      case Throw(e) =>
        exec.execute(new Runnable { def run(): Unit = handleCloseWith(e) })
    }

    private[this] def handleWriteAndCheck(obj: HttpObject): Future[Unit] = {
      state match {
        case a: Active =>
          if (obj.isInstanceOf[LastHttpContent]) {
            handleState(a.copy(finishedWriting = true))
            handleCheckFinished()
          }

          underlying
            .write(Message(obj, curId))
            .onFailure { e: Throwable =>
              exec.execute(new Runnable { def run(): Unit = handleCloseWith(e) })
            }

        case _ =>
          _onClose.unit
      }
    }

    def write(obj: HttpObject): Future[Unit] = {
      val writep = new Promise[Unit]
      exec.execute(new Runnable {
        def run(): Unit = {
          val result: Future[Unit] = state match {
            case Idle =>
              // parent.dead is only examined when starting or finishing a stream
              if (!parent.dead) {
                handleNewStream()
                handleWriteAndCheck(obj)
              } else {
                log.warning("Write to child with dead parent")
                handleCloseStream("tried to write to a child with a dead parent")
                _onClose.unit
              }

            case state if obj.isInstanceOf[HttpRequest] =>
              handleBadState(s"Writing request prelude when not in Idle state: $state. id: $curId")

            case a: Active if a.finishedWriting =>
              handleBadState(s"Write after finished writing: $obj")

            case a: Active =>
              handleWriteAndCheck(obj)

            case Dead =>
              handleBadState(s"Write to dead child: $obj")
          }
          result.proxyTo(writep)
        }
      })
      writep
    }

    private[this] val closeOnInterrupt: PartialFunction[Throwable, Unit] = {
      case t: Throwable =>
        exec.execute(new Runnable { def run(): Unit = handleCloseWith(t) })
    }

    def read(): Future[HttpObject] = {
      val readp = new Promise[HttpObject]
      exec.execute(new Runnable {
        def run(): Unit = {
          state match {
            case _: Active =>
              val result = queue.poll()

              result.poll match {
                case Some(res) =>
                  readp.updateIfEmpty(res)
                  postRead(res)

                case None =>
                  result.respond(postRead)
                  result.proxyTo(readp)
                  readp.setInterruptHandler(closeOnInterrupt)
              }

            case Dead =>
              queue.poll().proxyTo(readp)

            case Idle =>
              val drained = queue.drain()
              handleBadState(s"Read from idle child. Drained $drained")
                .asInstanceOf[Future[HttpObject]]
                .proxyTo(readp)
          }
        }
      })
      readp
    }

    private[MultiplexedTransporter] def handleOffer(obj: HttpObject): Unit = {
      state match {
        case a: Active if !a.finishedReading =>
          if (obj.isInstanceOf[LastHttpContent]) {
            handleState(a.copy(finishedReading = true))
          }

          if (!queue.offer(obj)) {
            handleBadState(s"Failed to enqueue message. Queue size: ${queue.size}, msg: $obj")
          }

        case _: Active =>
          // Technically, this condition is a protocol or stream error depending on
          // whether the stream is fully or only half closed, but we are going to be
          // lenient right now as our server implementation may be doing this and we
          // don't want to clobber any other active streams until the server is
          // behaving. See https://tools.ietf.org/html/rfc7540#section-5.1
          log.warning(s"Received message on inbound-closed stream: ($obj)")

        case Idle =>
          handleBadState(s"Offered message to idle child: $obj")

        case Dead =>
          handleBadState(s"Offered message to dead child: $obj")
      }
    }

    def status: Status = {
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
      exec.execute(new Runnable { def run(): Unit = handleCloseStream("close called on child transport", deadline) })
      _onClose.unit
    }

    private[http2] def handleCloseStream(whyFailed: String, deadline: Time = Time.Bottom): Unit = {
      handleCloseWith(new StreamClosedException(Some(addr), _curId.toString, whyFailed), deadline)
    }

    private[http2] def handleCloseWith(exn: Throwable, deadline: Time = Time.Bottom): Unit = {
      state match {
        case a: Active if !a.finished =>
          underlying
            .write(Rst(_curId, Http2Error.CANCEL.code))
            .by(deadline)(DefaultTimer) // TODO: Get Timer from stack params
        case _ =>
      }

      handleState(Dead)
      children.remove(curId)

      queue.fail(exn, discard = false)
      _onClose.updateIfEmpty(Return(exn))
    }

    val context: TransportContext = new LegacyContext(child)
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
  object Idle extends ChildState {
    override def toString: String = "Idle"
  }

  /**
   * Child represents a stream with active reading/writing
   */
  case class Active(finishedReading: Boolean, finishedWriting: Boolean) extends ChildState {
    def finished = finishedWriting && finishedReading
    override def toString: String = {
      if (finished)
        "Active(finished)"
      else if (finishedReading)
        "Active(writing)"
      else if (finishedWriting)
        "Active(reading)"
      else
        "Active(reading/writing)"
    }
  }

  /**
   * Child is closed/dead and cannot be used again
   */
  object Dead extends ChildState {
    override def toString: String = "Dead"
  }

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
