package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.{Service, Stack, Status, param}
import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.mux.ServerDispatcher.State
import com.twitter.finagle.mux.{Request, Response, gracefulShutdownEnabled}
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.mux.transport.Message.Tags
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
  decoder: MuxMessageDecoder,
  messageWriter: MessageWriter,
  handle: PushChannelHandle[ByteReader, Buf], // Maybe we should refine into a PushClientConnection...
  service: Service[Request, Response]
) extends PushSession[ByteReader, Buf](handle) {
  import MuxServerSession._

  // These are the locals we need to have set on each dispatch
  private[this] def locals: Local.Context =
    Local.letClear {
      val remoteAddressLocal = Contexts.local
        .KeyValuePair(RemoteInfo.Upstream.AddressCtx, handle.remoteAddress)
      val peerCertLocal = handle.peerCertificate.map(
        Contexts.local.KeyValuePair(Transport.peerCertCtx, _))

      Trace.letTracer(params[param.Tracer].tracer) {
        Contexts.local.let(Seq(remoteAddressLocal) ++ peerCertLocal) {
          Local.save()
        }
      }
    }

  private[this] val exec = handle.serialExecutor
  private[this] val lessor = params[Lessor.Param].lessor
  private[this] val statsReceiver = params[param.Stats].statsReceiver
  private[this] val tracker = new ServerTracker(
    exec, locals, service, messageWriter, lessor, statsReceiver, handle.remoteAddress)

  // Must only be modified from within the session executor, but can be
  // observed concurrently via status.
  // State transitions can only go Open -> Draining -> Closed, potentially skipping Draining.
  @volatile
  private[this] var dispatchState: State.Value = State.Open

  lessor.register(tracker)

  // Hook up the shutdown logic from the tracker and handle.
  tracker.drained.respond { result =>
    exec.execute(new Runnable { def run(): Unit = handleShutdown(result) })
  }

  handle.onClose.respond { result =>
    exec.execute(new Runnable { def run(): Unit = handleShutdown(result) })
  }

  def receive(reader: ByteReader): Unit = {
    try {
      val message = decoder.decode(reader)
      if (message != null) processMessage(message)
    } catch { case NonFatal(t) =>
      handleShutdown(Throw(t))
    }
  }

  private[this] def processMessage(message: Message): Unit = message match {
    case m: Message.Tdispatch =>
      tracker.dispatch(m)

    case m: Message.Treq =>
      tracker.dispatch(m)

    case Message.Tping(tag) =>
      val response =
        if (tag == Tags.PingTag) Message.PreEncoded.Rping
        else Message.Rping(tag)
      messageWriter.write(response)

    case Message.Tdiscarded(tag, why) =>
      tracker.discarded(tag, why)

    case Message.Rdrain(Tags.ControlTag) if dispatchState == State.Draining =>
      log.debug("Received Rdrain")
      tracker.drain()

    // We should not expect fragments as they are accumulated by the decoder
    case other =>
      val rerror = Message.Rerr(other.tag, s"Unexpected mux message type ${other.typ}")
      messageWriter.write(rerror)
  }

  def status: Status = {
    val stateStatus = dispatchState match {
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
    if (dispatchState == State.Open) {
      dispatchState = State.Draining

      if (!gracefulShutdownEnabled()) handleShutdown(Return.Unit)
      else {
        log.debug("Draining session")
        statsReceiver.counter("draining").incr()

        messageWriter.write(Message.Tdrain(Tags.ControlTag))

        // We set a timer to make sure we've drained within the allotted amount of time
        tracker.drained.by(params[param.Timer].timer, deadline).respond {
          case Throw(_: TimeoutException) =>
            exec.execute(new Runnable { def run(): Unit = {
              val t = new TimeoutException(
                s"Failed to drain within the deadline $deadline. Tracker state: ${tracker.currentState}")
              handleShutdown(Throw(t))
            } })

          case _ => // nop: didn't timeout, so it should have already been handled
        }
      }
    }
  }

  // All shutdown pathways should flow through this method.
  private[this] def handleShutdown(reason: Try[Unit]): Unit = {
    if (dispatchState != State.Closed) {
      dispatchState = State.Closed
      lessor.unregister(tracker)
      tracker.interruptOutstandingDispatches()
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
}
