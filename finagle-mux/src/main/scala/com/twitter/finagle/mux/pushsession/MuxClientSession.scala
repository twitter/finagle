package com.twitter.finagle.mux.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle._
import com.twitter.finagle.mux.ReqRepFilter
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.Response
import com.twitter.finagle.mux.ServerError
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.pushsession.PushChannelHandle
import com.twitter.finagle.pushsession.PushSession
import com.twitter.finagle.mux.ReqRepFilter.CanDispatch
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.Verbosity
import com.twitter.io.Buf
import com.twitter.io.ByteReader
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.Executor
import scala.util.control.NonFatal

/**
 * Push based mux client implementation based on the push-based abstractions.
 *
 * This session is expected to be fully configured and doesn't attempt to
 * perform negotiation other than inferring whether Tdispatch/Rdispatch messages
 * are supported by the server.
 *
 * The core principle of the `MuxClientSession` is that the session is in control
 * of the concurrency model and (most) decisions, predominantly through ensure valid
 * state for operations and routing events. This allows essentially all stateful
 * concerns, such as managing the data plane, to be implemented without propagating
 * concurrency concerns into their implementations.
 *
 * The `ClientTracker` takes responsibility for tracking state related to dispatches
 * and leaves other session layer concerns such as pings, leases, and ensuring that
 * the session layer is ready to dispatch to its parent `MuxClientSession`. If the mux
 * protocol is expanded to support streaming the extra complexity of managing the
 * individual stream states would likely live in the `ClientTracker` and flow control
 * would similarly be abstracted to a data structure that belongs to the session.
 */
private[finagle] final class MuxClientSession(
  handle: PushChannelHandle[ByteReader, Buf],
  h_decoder: MuxMessageDecoder,
  h_messageWriter: MessageWriter,
  detectorConfig: FailureDetector.Config,
  name: String,
  statsReceiver: StatsReceiver)
    extends PushSession[ByteReader, Buf](handle) {
  import MuxClientSession._

  // Volatile only to ensure prompt visibility from the synchronous
  // `status` method and `leaseGauge`.
  // Dispatch state represents the lifecycle of the session. The allowed transitions are
  //      [ Dispatching -> Leasing -> Draining -> Drained ]
  // Drained represents the terminal phase of the session, and is only set in the
  // `handleShutdown` method. The session will only dispatch while in the Dispatching
  // and Leasing states.
  @volatile private[this] var h_dispatchState: State = Dispatching
  @volatile private[this] var h_canDispatch: CanDispatch.State = CanDispatch.Unknown
  private[this] var h_pingPromise: Promise[Unit] = null
  private[this] val h_tracker = new ClientTracker(h_messageWriter)

  private[this] val log: Logger = Logger.get(getClass.getName)
  private[this] val exec: Executor = handle.serialExecutor

  private[this] val failureDetector =
    FailureDetector(detectorConfig, ping _, statsReceiver.scope("failuredetector"))

  // Metrics
  private[this] val leaseCounter = statsReceiver.counter(Verbosity.Debug, "leased")
  private[this] val drainingCounter = statsReceiver.counter("draining")
  private[this] val drainedCounter = statsReceiver.counter("drained")

  // exposed for testing
  private[pushsession] def currentLease: Option[Duration] = h_dispatchState match {
    case l: Leasing => Some(l.remaining)
    case _ => None
  }

  private[this] def isDraining: Boolean = h_dispatchState match {
    case Draining | Drained => true
    case _ => false
  }

  private[this] def isDrained: Boolean = h_dispatchState == Drained

  // Implementation of the Service to interface with the session
  private[this] object MuxClientServiceImpl extends Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      if (h_canDispatch != CanDispatch.Unknown) {
        dispatch(request)
      } else {
        // We don't know yet if we can Tdispatch, so the first try may fail
        // and if so we know it didn't get processed and we can try again.
        dispatch(request).transform {
          case Throw(ServerError(_)) =>
            // We've determined that the server cannot handle Tdispatch messages,
            // so we fall back to a Treq.
            h_canDispatch = CanDispatch.No
            apply(request)

          case m @ Return(_) =>
            h_canDispatch = CanDispatch.Yes
            Future.const(m)

          case t @ Throw(_) =>
            Future.const(t.cast[Response])
        }
      }
    }

    override def close(deadline: Time): Future[Unit] = MuxClientSession.this.close(deadline)

    override def status: Status = MuxClientSession.this.status
  }

  handle.onClose.respond { result =>
    exec.execute(new Runnable {
      def run(): Unit = result match {
        case Return(_) => handleShutdown(None)
        case Throw(t) => handleShutdown(Some(t))
      }
    })
  }

  def close(deadline: Time): Future[Unit] = {
    // We only close the socket connection on via explicit `close` calls to reproduce what
    // happens in the transport/dispatcher model.
    handle.close(deadline)
  }

  def receive(reader: ByteReader): Unit = {
    // We decode every `ByteReader` explicitly to ensure any underlying resources
    // are released by the decoder whether we intend to use the result or not.
    try {
      val message = h_decoder.decode(reader)
      // If the session isn't open, we just drop the message since the waiters were cleared
      if (message != null && !isDrained) {
        handleProcessMessage(message)
      }
    } catch {
      case NonFatal(t) =>
        handleShutdown(Some(t))
    }
  }

  /** Dispatch a client request */
  private[this] def dispatch(request: Request): Future[Response] = {
    val p = Promise[Response]()
    val locals = Local.save() // Need this for Trace, Dtab, and broadcast context info.
    exec.execute(new Runnable { def run(): Unit = handleDispatch(request, locals, p) })
    p
  }

  // Entry point for dispatching once we're in the serialExecutor.
  // This method has the responsibility to make sure we can dispatch and then perform
  // the dispatch check, if necessary.
  private[this] def handleDispatch(
    req: Request,
    locals: Local.Context,
    dispatchP: Promise[Response]
  ): Unit = {
    if (isDraining) dispatchP.setException(Failure.RetryableNackFailure)
    else {
      val asDispatch = h_canDispatch != CanDispatch.No
      val tag = h_tracker.dispatchRequest(req, asDispatch, locals, dispatchP)
      dispatchP.setInterruptHandler {
        case cause: Throwable =>
          exec.execute(new Runnable {
            def run(): Unit = {
              h_tracker.requestInterrupted(dispatchP, tag, cause)
            }
          })
      }
    }
  }

  /**
   * Send a PING request to the Server which will resolve the Future when
   * the response has been received.
   *
   * @note If a Ping request is already outstanding, the new PING request will fail.
   */
  def ping(): Future[Unit] = {
    val pp = Promise[Unit]()
    exec.execute(new Runnable {
      def run(): Unit = {
        if (isDrained || h_pingPromise != null) FuturePingNack.proxyTo(pp)
        else {
          h_pingPromise = pp
          h_messageWriter.write(Message.PreEncoded.Tping)
        }
      }
    })
    pp
  }

  def asService: Future[Service[Request, Response]] = Future.value(MuxClientServiceImpl)

  def status: Status = {
    // Return the worst status reported among
    // - FailureDetector
    // - Handle status
    // - Dispatch state status
    Status.worst(failureDetector.status, Status.worst(handle.status, dispatchStateToStatus))
  }

  // This is not a method that is always called from within the serial executor but
  // it's safe to _read_ the `h_dispatchState` field since the usage is racy and the
  // field is marked volatile to ensure prompt visibility of updates.
  private[this] def dispatchStateToStatus: Status = h_dispatchState match {
    case leased: Leasing if leased.expired => Status.Busy
    case Leasing(_) | Dispatching => Status.Open
    case Draining => Status.Busy
    case Drained => Status.Closed
  }

  private[this] def handleShutdown(oexc: Option[Throwable]): Unit = {
    if (h_dispatchState != Drained) {
      // The client doesn't force the connection close, instead it just transitions
      // to the `Drained` state, and that is reflected in the `status` as `Closed`
      // The underlying socket isn't actually closed by the session but waits for an
      // external call to `.close()`

      h_dispatchState = Drained

      // Fail an outstanding Ping, if it exists
      if (h_pingPromise != null) {
        val pp = h_pingPromise
        h_pingPromise = null
        FuturePingNack.proxyTo(pp)
      }

      h_tracker.shutdown(oexc, Some(handle.remoteAddress))

      oexc match {
        case Some(t) => log.info(t, s"Closing mux client session to $name due to error")
        case None => ()
      }
    }
  }

  private[this] def handleProcessMessage(msg: Message): Unit = {
    msg match {
      case Message.Tping(Message.Tags.PingTag) =>
        h_messageWriter.write(Message.PreEncoded.Rping)

      case Message.Tping(tag) =>
        h_messageWriter.write(Message.Rping(tag))

      case p @ Message.Rping(_) =>
        if (h_pingPromise == null) {
          log.info(s"($name) Received unexpected ping response: $p")
        } else {
          val p = h_pingPromise
          h_pingPromise = null
          p.setDone()
        }

      case Message.Rerr(_, err) =>
        log.info(s"($name) Server error: $err")
        h_tracker.receivedResponse(msg.tag, Throw(ServerError(err)))

      case Message.Rmessage(_) =>
        h_tracker.receivedResponse(msg.tag, ReqRepFilter.reply(msg))

      case Message.Tdrain(tag) =>
        // Ack the Tdrain and begin shutting down.
        h_messageWriter.write(Message.Rdrain(tag))
        h_dispatchState = Draining
        drainingCounter.incr()
        if (log.isLoggable(Level.TRACE))
          log.trace(s"Started draining a connection to $name")

      case Message.Tlease(Message.Tlease.MillisDuration, millis) =>
        h_dispatchState match {
          case Leasing(_) | Dispatching =>
            h_dispatchState = Leasing(Time.now + millis.milliseconds)
            if (log.isLoggable(Level.DEBUG))
              log.debug(s"($name) leased for ${millis.milliseconds} to $name")
            leaseCounter.incr()
          case Draining | Drained =>
          // Ignore the lease if we're closed, since these are irrecoverable states.
        }

      case other =>
        handleShutdown(Some(new IllegalStateException(s"Unexpected message: $other")))
    }

    // Check if we're in a finished-draining state and transition accordingly
    if (h_dispatchState == Draining && h_tracker.pendingDispatches == 0) {
      if (log.isLoggable(Level.TRACE)) {
        log.trace(s"Finished draining a connection to $name")
      }

      drainedCounter.incr()
      handleShutdown(None)
    }
  }
}

private object MuxClientSession {
  private val FuturePingNack: Future[Nothing] =
    Future.exception(Failure("A ping is already outstanding on this session."))

  private sealed trait State
  private case object Dispatching extends State
  private case object Draining extends State
  private case object Drained extends State
  private case class Leasing(end: Time) extends State {
    def remaining: Duration = end.sinceNow
    def expired: Boolean = end < Time.now
  }
}
