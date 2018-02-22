package com.twitter.finagle.mux.exp.pushsession

import com.twitter.conversions.time._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle._
import com.twitter.finagle.mux.ClientSession.{Dispatching, Drained, Draining, Leasing}
import com.twitter.finagle.mux.{ClientSession, ReqRepFilter, Request, Response, ServerError}
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.mux.ReqRepFilter.CanDispatch
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.{Level, Logger}
import com.twitter.util._
import java.util.concurrent.Executor

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
  decoder: MuxMessageDecoder,
  messageWriter: MessageWriter,
  detectorConfig: FailureDetector.Config,
  name: String,
  statsReceiver: StatsReceiver,
  timer: Timer
) extends PushSession[ByteReader, Buf](handle) {

  // Volatile only to ensure prompt visibility from the synchronous
  // `status` method and `leaseGauge`.
  // Dispatch state represents the lifecycle of the session. The allowed transitions are
  //      [ Dispatching -> Leasing -> Draining -> Drained ]
  // Drained represents the terminal phase of the session, and is only set in the
  // `handleShutdown` method. The session will only dispatch while in the Dispatching
  // and Leasing states.
  @volatile
  private[this] var dispatchState: ClientSession.State = ClientSession.Dispatching

  @volatile
  private[this] var canDispatch: CanDispatch.State = CanDispatch.Unknown

  private[this] val log: Logger = Logger.get(getClass.getName)
  private[this] val exec: Executor = handle.serialExecutor
  private[this] val tracker = new ClientTracker(messageWriter)

  private[this] val failureDetector =
    FailureDetector(detectorConfig, ping, statsReceiver.scope("failuredetector"))

  // All modifications to session state must be done from within the serial executor
  private[this] var pingPromise: Promise[Unit] = null

  // Metrics
  private[this] val leaseCounter = statsReceiver.counter(Verbosity.Debug, "leased")
  private[this] val drainingCounter = statsReceiver.counter("draining")
  private[this] val drainedCounter = statsReceiver.counter("drained")

  // exposed for testing
  private[pushsession] def currentLease: Option[Duration] = dispatchState match {
    case l: Leasing => Some(l.remaining)
    case _ => None
  }

  private[this] def isDraining: Boolean = dispatchState match {
    case Draining | Drained => true
    case _ => false
  }

  private[this] def isDrained: Boolean = dispatchState == Drained

  // Implementation of the Service to interface with the session
  private[this] object MuxClientServiceImpl extends Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      if (canDispatch != CanDispatch.Unknown) {
        dispatch(request)
      } else {
        // We don't know yet if we can Tdispatch, so the first try may fail
        // and if so we know it didn't get processed and we can try again.
        dispatch(request).transform {
          case Throw(ServerError(_)) =>
            // We've determined that the server cannot handle Tdispatch messages,
            // so we fall back to a Treq.
            canDispatch = CanDispatch.No
            apply(request)

          case m @ Return(_) =>
            canDispatch = CanDispatch.Yes
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
      val message = decoder.decode(reader)
      // If the session isn't open, we just drop the message since the waiters were cleared
      if (message != null && !isDrained) {
        handleProcessMessage(message)
      }
    } catch { case NonFatal(t) =>
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
      val asDispatch = canDispatch != CanDispatch.No
      val tag = tracker.dispatchRequest(req, asDispatch, locals, dispatchP)
      dispatchP.setInterruptHandler {
        case cause: Throwable =>
          exec.execute(new Runnable {
            def run(): Unit = {
              tracker.requestInterrupted(dispatchP, tag, cause)
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
        if (isDrained || pingPromise != null) ClientSession.FuturePingNack.proxyTo(pp)
        else {
          pingPromise = pp
          messageWriter.write(Message.PreEncoded.Tping)
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

  private[this] def dispatchStateToStatus: Status = dispatchState match {
    case leased: Leasing if leased.expired => Status.Busy
    case Leasing(_) | Dispatching => Status.Open
    case Draining => Status.Busy
    case Drained => Status.Closed
  }

  private[this] def handleShutdown(oexc: Option[Throwable]): Unit = {
    if (dispatchState != Drained) {
      // The client doesn't force the connection close, instead it just transitions
      // to the `Drained` state, and that is reflected in the `status` as `Closed`
      // The underlying socket isn't actually closed by the session but waits for an
      // external call to `.close()`

      dispatchState = Drained

      // Fail an outstanding Ping, if it exists
      if (pingPromise != null) {
        val pp = pingPromise
        pingPromise = null
        ClientSession.FuturePingNack.proxyTo(pp)
      }

      tracker.shutdown(oexc, Some(handle.remoteAddress))

      oexc match {
        case Some(t) => log.info(t, s"Closing mux client session to $name due to error")
        case None => ()
      }
    }
  }

  private[this] def handleProcessMessage(msg: Message): Unit = {
    msg match {
      case Message.Tping(Message.Tags.PingTag) =>
        messageWriter.write(Message.PreEncoded.Rping)

      case Message.Tping(tag) =>
        messageWriter.write(Message.Rping(tag))

      case p @ Message.Rping(_) =>
        if (pingPromise == null) {
          log.info(s"($name) Received unexpected ping response: $p")
        } else {
          val p = pingPromise
          pingPromise = null
          p.setDone()
        }

      case Message.Rerr(_, err) =>
        log.info(s"($name) Server error: $err")
        tracker.receivedResponse(msg.tag, Throw(ServerError(err)))

      case Message.Rmessage(_) =>
        tracker.receivedResponse(msg.tag, ReqRepFilter.reply(msg))

      case Message.Tdrain(tag) =>
        // Ack the Tdrain and begin shutting down.
        messageWriter.write(Message.Rdrain(tag))
        dispatchState = Draining
        drainingCounter.incr()
        if (log.isLoggable(Level.TRACE))
          log.trace(s"Started draining a connection to $name")

      case Message.Tlease(Message.Tlease.MillisDuration, millis) =>
        dispatchState match {
          case Leasing(_) | Dispatching =>
            dispatchState = Leasing(Time.now + millis.milliseconds)
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
    if (dispatchState == Draining && tracker.pendingDispatches == 0) {
      if (log.isLoggable(Level.TRACE)) {
        log.trace(s"Finished draining a connection to $name")
      }

      drainedCounter.incr()
      handleShutdown(None)
    }
  }
}
