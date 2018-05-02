package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http.TooLongMessageException
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{Verbosity, VerbosityAdjustingStatsReceiver}
import com.twitter.finagle.transport.{LegacyContext, Transport, TransportContext}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Failure, FailureFlags, Stack, Status, StreamClosedException}
import com.twitter.logging.{HasLogLevel, Level, Logger}
import com.twitter.util.{Closable, Future, Promise, Return, Throw, Time, Try}
import io.netty.handler.codec.http.{HttpObject, HttpRequest, LastHttpContent}
import io.netty.handler.codec.http2.Http2Error
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * A factory for making a transport which represents an http2 stream.
 *
 * After a stream finishes, the transport represents the next stream id.
 *
 * Since each transport only represents a single stream at a time, one of these
 * transports cannot be multiplexed-instead, the [[StreamTransport]] is the
 * unit of multiplexing, so if you want to increase concurrency, you should
 * create a new Transport.
 *
 * ==Threading==
 *
 * [[StreamTransportFactory]] requires the underlying transport to provide a serial
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
final private[http2] class StreamTransportFactory(
  underlying: Transport[StreamMessage, StreamMessage] {
    type Context = TransportContext with HasExecutor
  },
  addr: SocketAddress,
  params: Stack.Params
) extends (() => Future[Transport[HttpObject, HttpObject]])
    with Closable { parent =>
  import StreamTransportFactory._

  private[this] val exec = underlying.context.executor
  private[this] val log = Logger.get(getClass.getName)

  // A map of active streamIds -> StreamTransport. Concurrency issues are handled by the serial
  // executor.
  private[this] val activeStreams = new MutableHashMap[Int, StreamTransport]()

  // If it's not priorknowledge or tls/alpn, then it must be h2c
  private[this] val isH2c =
    !(params[PriorKnowledge].enabled || params[Transport.ClientSsl].sslClientConfiguration.isDefined)

  // For non-H2C sessions we pick a higher initial id to avoid an NPE which can
  // happen when the first stream is deregistered. see: https://github.com/netty/netty/issues/7898
  private[this] val id = if (isH2c) new AtomicInteger(1) else new AtomicInteger(3)

  // This state as well as operations that start or stop streams and goaways are serialized via `exec`
  @volatile private[this] var dead = false
  @volatile private[this] var pingPromise: Promise[Unit] = null

  // exposed for testing & streams gauge synchronized because this is called outside of the executor
  private[http2] def numActiveStreams: Int = synchronized { activeStreams.size }
  private[http2] def setStreamId(num: Int): Unit = id.set(num)
  private[http2] def removeStream(num: Int): Unit = activeStreams.remove(num)

  private[this] val FailureDetector.Param(detectorConfig) = params[FailureDetector.Param]
  private[this] val Stats(statsReceiver) = params[Stats]

  private[this] val activeStreamsGauge = statsReceiver.addGauge("streams") { numActiveStreams }

  private[this] val debugStats = new VerbosityAdjustingStatsReceiver(statsReceiver, Verbosity.Debug)
    .scope("debug")

  private val removeIdleCounter = debugStats.counter("remove_idle")
  private val removeRstCounter = debugStats.counter("remove_rst")
  private val removeExnCounter = debugStats.counter("remove_exn")
  private val removeCloseCounter = debugStats.counter("remove_close")

  private def handleRemoveStream(streamId: Int): Boolean = {
    activeStreams.remove(streamId).isDefined
  }

  // exposed for testing
  private[http2] def ping(): Future[Unit] = {
    val done = new Promise[Unit]
    exec.execute(new Runnable {
      def run(): Unit = {
        if (pingPromise == null) {
          pingPromise = done
          underlying.write(Ping)
        } else {
          done.setException(PingOutstandingFailure)
        }
      }
    })
    done
  }

  private[this] val detector =
    FailureDetector(detectorConfig, ping, statsReceiver.scope("failuredetector"))

  // H2 uses the default WatermarkPool, which believes each StreamTransport
  // represents a connection. When the WatermarkPool sees a peer marked "Closed",
  // it believes the connection has already been torn down and doesn't make an
  // attempt to close it. Therefore, we ensure that if the FailureDetector marks
  // this connection as closed, it gets torn down.
  detector.onClose.ensure(close())

  private[this] def handleGoaway(obj: HttpObject, lastStreamId: Int): Unit = {
    dead = true
    activeStreams.values.foreach { stream =>
      if (stream.curId > lastStreamId) {
        stream
          .handleCloseStream(
            s"the stream id (${stream.curId}) was higher than the last stream" +
              s" id ($lastStreamId) on a GOAWAY"
          )
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
      activeStreams.get(streamId) match {
        case Some(stream) =>
          stream.handleOffer(msg)

        case None =>
          val lastId = id.get()
          if (log.isLoggable(Level.DEBUG))
            log.debug(
              s"Got message for nonexistent stream $streamId. Next client " +
                s"stream id=${lastId}. msg=$msg"
            )

          if (streamId < id.get()) {
            // This stream may have existed and was closed. This is an error, but is recoverable.
            underlying.write(Rst(streamId, Http2Error.STREAM_CLOSED.code))
          } else {
            // This stream definitely has not yet existed. This error is not recoverable.
            underlying.write(
              // TODO: Properly utilize the DEBUG_DATA section of GOAWAY
              GoAway(
                LastHttpContent.EMPTY_LAST_CONTENT,
                lastId,
                Http2Error.PROTOCOL_ERROR.code
              )
            )
            handleClose(
              Time.Bottom,
              Some(new Http2ProtocolException(s"Message for streamId $streamId which doesn't exist yet"))
            )
          }
      }

    case GoAway(msg, lastStreamId, _) =>
      handleGoaway(msg, lastStreamId)

    case Rst(streamId, errorCode) =>
      val error =
        if (errorCode == Http2Error.REFUSED_STREAM.code) Failure.RetryableNackFailure
        else if (errorCode == Http2Error.ENHANCE_YOUR_CALM.code) Failure.NonRetryableNackFailure
        else new StreamClosedException(addr, streamId.toString)

      activeStreams.get(streamId) match {
        case Some(stream) =>
          handleRemoveStream(streamId)
          removeRstCounter.incr()
          stream.handleCloseWith(error, canRst = false)
        case None =>
          // According to spec, an endpoint should not send another RST upon receipt
          // of an RST for an absent stream ID as this could cause a loop.
          if (log.isLoggable(Level.DEBUG))
            log.debug(s"Got RST for nonexistent stream: $streamId, code: $errorCode")
      }

    case StreamException(exn, streamId) =>
      val error = TooLongMessageException(exn, addr)
      activeStreams.get(streamId) match {
        case Some(stream) =>
          handleRemoveStream(streamId)
          stream.handleCloseWith(error, canRst = false)
        case None =>
          if (log.isLoggable(Level.DEBUG))
            log.debug(exn, s"Got exception for nonexistent stream: $streamId")
      }
    case Ping =>
      if (pingPromise != null) {
        pingPromise.setDone()
        pingPromise = null
      } else {
        log.debug(s"Got unmatched PING message for address $addr")
      }

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
      def run(): Unit = result match {
        case Return(msg) =>
          handleSuccessfulRead(msg)
          loop()
        case Throw(e) =>
          handleClose(Time.Bottom, Some(e))
      }
    })
  }

  private[this] def loop(): Unit = underlying.read().respond(readLoop)

  loop()

  // ensure that `first` is only called once.
  private[this] val firstOnce = new AtomicBoolean(false)

  /**
   * Provides a transport that knows we've already sent the first request, so we
   * just need a response to advance to the next stream.
   *
   * @note this method is not thread-safe as the [[StreamTransport]] instance can modify
   *       [[StreamTransportFactory]] state concurrently to any instances produced via
   *       `apply`.
   */
  def first(): Transport[HttpObject, HttpObject] = {
    if (firstOnce.compareAndSet(false, true)) {
      val st = new StreamTransport()
      st.handleNewStream()
      st.handleState(Active(finishedWriting = true, finishedReading = false))
      st
    } else {
      throw new IllegalStateException(s"$this.first() was called multiple times")
    }
  }

  def apply(): Future[Transport[HttpObject, HttpObject]] = {
    val p = new Promise[Transport[HttpObject, HttpObject]]
    exec.execute(new Runnable {
      def run(): Unit = {
        if (dead) p.setException(new DeadConnectionException(addr, FailureFlags.Retryable))
        else {
          val st = new StreamTransport()
          p.setValue(st)
        }
      }
    })
    p
  }

  def onClose: Future[Throwable] = underlying.context.onClose

  private[this] def handleClose(
      deadline: Time,
      streamExn: Option[Throwable] = None
    ): Unit = {
    dead = true
    activeStreams.values.foreach { stream =>
      streamExn match {
        case Some(e) =>
          stream.handleCloseWith(e, deadline)
        case None =>
          stream.handleCloseStream("StreamTransportFactory closed", deadline)
      }
    }
    underlying.close(deadline)
  }

  def close(deadline: Time): Future[Unit] = {
    exec.execute(new Runnable {
      def run(): Unit = handleClose(deadline)
    })
    underlying.context.onClose.unit
  }

  // Ensure we report closed if closed has been called but the detector has not yet been triggered
  def status: Status = if (dead) Status.Closed else detector.status

  /**
   * StreamTransport represents a single http/2 stream at a time.  Once the stream
   * has finished, the transport can be used again for a different stream.
   */
  // One note is that closing one of the transports closes the underlying
  // transport.  This doesn't play well with configurations that close idle
  // connections, since finagle doesn't know that these are streams, not
  // connections.  This can be improved by adding an extra layer between pooling
  // and dispatching, or making the distinction between streams and sessions
  // explicit.
  private[http2] class StreamTransport extends Transport[HttpObject, HttpObject] { stream =>
    type Context = TransportContext

    private[this] val _onClose: Promise[Throwable] = new Promise[Throwable]

    // Current stream ID
    @volatile private[this] var _curId = 0

    // Exposed for testing
    private[http2] def curId: Int = _curId

    private[this] val queue: AsyncQueue[HttpObject] = new AsyncQueue[HttpObject]

    @volatile private[StreamTransportFactory] var _state: StreamState = Idle

    private[StreamTransportFactory] def handleState(newState: StreamState): Unit = {
      if (log.isLoggable(Level.DEBUG)) {
        log.debug(s"Stream $curId: ${_state} => ${newState}")
      }
      _state = newState
    }

    def state: StreamState = _state

    private[StreamTransportFactory] def handleNewStream(): Unit = state match {
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
            activeStreams.put(newId, stream)
            _curId = newId
          }

          // TODO: It is also a problem if someone is already polling on the queue in an idle
          //       state. This represents a premature read on a stream that is not yet associated
          //       with a stream ID.
        }

      case _ =>
        handleBadState(
          s"newStream in state: ${state.getClass.getName}, parent dead? ${parent.dead}"
        )
    }

    private[this] def handleBadState(msg: String): Future[Nothing] = {
      log.error(s"Stream ${_curId} bad state: $msg")
      val exn = new BadStreamStateException(msg, curId)

      // If this is not in activeStreams, it was removed for another reason
      if (handleRemoveStream(curId)) removeExnCounter.incr()
      handleCloseWith(exn)
      Future.exception(exn)
    }

    private[this] def handleCheckFinished() = state match {
      case a: Active if a.finished && queue.size == 0 =>
        if (parent.dead) handleCloseStream(s"parent MultiplexedTransporter already dead")
        else {
          if (handleRemoveStream(curId)) {
            removeIdleCounter.incr()
            handleState(Idle)
          } else {
            handleBadState("Stream ID not found in map when going idle")
          }
        }

      case _ => // nop
    }

    private[this] val postReadReturnRunnable: Runnable =
      new Runnable { def run(): Unit = handleCheckFinished() }

    private[this] val postRead: Try[HttpObject] => Unit = {
      case Return(_) =>
        exec.execute(postReadReturnRunnable)

      case Throw(e) =>
        exec.execute(new Runnable { def run(): Unit = handleCloseWith(e) })
    }

    private[this] def handleWriteAndCheck(obj: HttpObject): Future[Unit] = state match {
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
                log.warning("Write to stream with dead parent")
                handleCloseStream("tried to write to a stream with a dead parent")
                _onClose.unit
              }

            case state if obj.isInstanceOf[HttpRequest] =>
              handleBadState(s"Writing request prelude when not in Idle state: $state. id: $curId")

            case a: Active if a.finishedWriting =>
              handleBadState(s"Write after finished writing: $obj")

            case _: Active =>
              handleWriteAndCheck(obj)

            case Dead =>
              handleBadState(s"Write to dead stream: $obj")
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
        def run(): Unit = state match {
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
            handleBadState(s"Read from idle stream. Drained $drained")
              .asInstanceOf[Future[HttpObject]]
              .proxyTo(readp)
        }
      })
      readp
    }

    private[StreamTransportFactory] def handleOffer(obj: HttpObject): Unit = {
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
          handleBadState(s"Offered message to idle stream: $obj")

        case Dead =>
          handleBadState(s"Offered message to dead stream: $obj")
      }
    }

    def status: Status = state match {
      case _: Active => Status.Open
      case Dead => Status.Closed
      case Idle => parent.status
    }

    def onClose: Future[Throwable] = _onClose.or(underlying.context.onClose)

    def localAddress: SocketAddress = underlying.context.localAddress

    def remoteAddress: SocketAddress = underlying.context.remoteAddress

    def peerCertificate: Option[Certificate] = underlying.context.peerCertificate

    def close(deadline: Time): Future[Unit] = {
      exec.execute(new Runnable {
        def run(): Unit = handleCloseStream("close called on stream transport", deadline)
      })
      _onClose.unit
    }

    private[http2] def handleCloseStream(whyFailed: String, deadline: Time = Time.Bottom): Unit = {
      handleCloseWith(new StreamClosedException(Some(addr), _curId.toString, whyFailed), deadline)
    }

    private[http2] def handleCloseWith(exn: Throwable, deadline: Time = Time.Bottom, canRst: Boolean = true): Unit = {
      state match {
        case a: Active if (!a.finished) && canRst =>
          underlying
            .write(Rst(_curId, Http2Error.CANCEL.code))
            .by(deadline)(DefaultTimer) // TODO: Get Timer from stack params
        case _ =>
      }

      handleState(Dead)
      // If this is not in activeStreams, it was removed for another reason
      if (handleRemoveStream(curId)) removeCloseCounter.incr()

      queue.fail(exn, discard = false)

      _onClose.updateIfEmpty(Return(exn))
    }

    val context: TransportContext = new LegacyContext(stream)
  }
}

private[http2] object StreamTransportFactory {
  val PingOutstandingFailure: Failure =
    Failure("A ping is already outstanding on this session.")

  val StreamMaxPendingOffers = 1000

  sealed trait StreamState

  /**
   * Stream is between requests and requires a new stream ID to continue
   */
  object Idle extends StreamState {
    override def toString: String = "Idle"
  }

  /**
   * Stream represents a stream with active reading/writing
   */
  private case class Active(finishedReading: Boolean, finishedWriting: Boolean) extends StreamState {
    def finished: Boolean = finishedWriting && finishedReading
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
   * Stream is closed/dead and cannot be used again
   */
  object Dead extends StreamState {
    override def toString: String = "Dead"
  }

  class Http2ProtocolException(msg: String) extends Exception(s"HTTP/2 Protocol error: $msg")

  class BadStreamStateException(
    msg: String,
    id: Int,
    private[finagle] val flags: Long = FailureFlags.NonRetryable
  ) extends Exception(s"Stream $id in bad state: $msg")
      with FailureFlags[BadStreamStateException] {

    protected def copyWithFlags(newFlags: Long): BadStreamStateException =
      new BadStreamStateException(msg, id, newFlags)
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
