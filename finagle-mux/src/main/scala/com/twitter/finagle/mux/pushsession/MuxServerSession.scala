package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.{CancelledRequestException, Mux, Service, Stack, Status, param}
import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message.Tags
import com.twitter.finagle.mux.{Request, Response}
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.param.Label
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.Logger
import com.twitter.util._
import scala.util.control.NonFatal

/**
 * Mux Server PushSession implementation
 */
private[finagle] final class MuxServerSession(
  params: Stack.Params,
  h_decoder: MuxMessageDecoder,
  h_messageWriter: MessageWriter,
  handle: PushChannelHandle[
    ByteReader,
    Buf
  ], // Maybe we should refine into a PushClientConnection...
  service: Service[Request, Response])
    extends PushSession[ByteReader, Buf](handle) {
  import MuxServerSession._

  // These are the locals we need to have set on each dispatch
  private[this] val locals: () => Local.Context = () =>
    Local.letClear {
      val remoteAddressLocal = Contexts.local
        .KeyValuePair(RemoteInfo.Upstream.AddressCtx, handle.remoteAddress)
      val peerCertLocal = Contexts.local
        .KeyValuePair(Transport.sslSessionInfoCtx, handle.sslSessionInfo)

      Trace.letTracer(params[param.Tracer].tracer) {
        Contexts.local.let(Seq(remoteAddressLocal, peerCertLocal)) {
          Local.save()
        }
      }
    }

  private[this] val exec = handle.serialExecutor
  private[this] val lessor = params[Lessor.Param].lessor
  private[this] val statsReceiver = params[param.Stats].statsReceiver
  private[this] val h_pingManager = params[Mux.param.PingManager].builder(exec, h_messageWriter)
  private[this] val h_tracker = new ServerTracker(
    exec,
    locals,
    service,
    h_messageWriter,
    lessor,
    statsReceiver,
    handle.remoteAddress
  )

  // Must only be modified from within the session executor, but can be
  // observed concurrently via status.
  // State transitions can only go Open -> Draining -> Closed, potentially skipping Draining.
  @volatile private[this] var h_dispatchState: State.Value = State.Open

  lessor.register(h_tracker.lessee)

  // Hook up the shutdown logic from the tracker and handle.
  h_tracker.drained.respond { result =>
    exec.execute(new Runnable { def run(): Unit = handleShutdown(result) })
  }

  handle.onClose.respond { result =>
    exec.execute(new Runnable { def run(): Unit = handleShutdown(result) })
  }

  def receive(reader: ByteReader): Unit = {
    try {
      val message = h_decoder.decode(reader)
      if (message != null) handleMessage(message)
    } catch {
      case NonFatal(t) =>
        handleShutdown(Throw(t))
    }
  }

  private[this] def handleMessage(message: Message): Unit = message match {
    case m: Message.Tdispatch =>
      h_tracker.dispatch(m)

    case m: Message.Treq =>
      h_tracker.dispatch(m)

    case Message.Tping(tag) =>
      h_pingManager.pingReceived(tag)

    case Message.Tdiscarded(tag, why) =>
      h_tracker.discarded(tag, why)

    case Message.Rdrain(Tags.ControlTag) if h_dispatchState == State.Draining =>
      log.debug("Received Rdrain")
      h_tracker.drain()

    // We should not expect fragments as they are accumulated by the decoder
    case other =>
      val rerror = Message.Rerr(other.tag, s"Unexpected mux message type ${other.typ}")
      h_messageWriter.write(rerror)
  }

  def status: Status = {
    val stateStatus = h_dispatchState match {
      case State.Open => Status.Open
      case State.Draining => Status.Busy
      case State.Closed => Status.Closed
    }
    Status.worst(handle.status, stateStatus)
  }

  def close(deadline: Time): Future[Unit] = {
    exec.execute(new Runnable { def run(): Unit = handleClose(deadline) })
    handle.onClose
  }

  private[this] def handleClose(deadline: Time): Unit = {
    // The we can only gracefully close once, and from the `Open` state.
    if (h_dispatchState == State.Open) {
      h_dispatchState = State.Draining

      log.debug("Draining session")

      // This stat is intentionally created on-demand as this event is infrequent enough to
      // outweigh the benefit of a persistent counter.
      statsReceiver.counter("draining").incr()

      h_messageWriter.write(Message.Tdrain(Tags.ControlTag))

      // We set a timer to make sure we've drained within the allotted amount of time
      h_tracker.drained.by(params[param.Timer].timer, deadline).respond {
        case Throw(_: TimeoutException) =>
          exec.execute(new Runnable {
            def run(): Unit = {
              val t = new TimeoutException(
                s"Failed to drain within the deadline $deadline. Tracker state: ${h_tracker.currentState}"
              )
              handleShutdown(Throw(t))
            }
          })

        case _ => // nop: didn't timeout, so it should have already been handled
      }
    }
  }

  // All shutdown pathways should flow through this method.
  private[this] def handleShutdown(reason: Try[Unit]): Unit = {
    if (h_dispatchState != State.Closed) {
      h_dispatchState = State.Closed
      lessor.unregister(h_tracker.lessee)
      // Construct the exception which we will use to satisfy any outstanding dispatches.
      // We wrap the underlying exc in a `CancelledRequestException` since it's the
      // historical API we've used to signal interrupted dispatches.
      val cause = new CancelledRequestException(reason match {
        case Return(_) => new Exception("mux server shutdown")
        case Throw(t) => t
      })
      h_tracker.interruptOutstandingDispatches(cause)
      Closable.all(handle, service).close()

      reason.onFailure { t =>
        val name = params[Label].label
        log.info(t, s"Server session ($name) closed due to error")
      }
    }
  }
}

private object MuxServerSession {
  private val log = Logger.get

  private object State extends Enumeration {
    val Open, Draining, Closed = Value
  }
}
