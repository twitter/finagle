package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.Failure
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Status
import com.twitter.finagle.mux.Handshake.CanTinitMsg
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.Handshake.TinitTag
import com.twitter.finagle.mux.pushsession.MuxClientNegotiatingSession._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.pushsession.PushChannelHandle
import com.twitter.finagle.pushsession.PushSession
import com.twitter.finagle.mux.Handshake
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.Verbosity
import com.twitter.io.Buf
import com.twitter.io.ByteReader
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/**
 * Allow interrupting mux client negotiation. This was only exposed as a temporary solution
 * to some ill effects we noticed in production and will be removed in the near future.
 * Setting this flag to false can be dangerous since there is no way to reclaim resources
 * during the negotiation phase if the peer, effectively, becomes a "black hole". Instead,
 * the more appropriate solution is to tune the service acquisition timeouts for the
 * respective client.
 */
private object allowInterruptingClientNegotiation
    extends com.twitter.app.GlobalFlag[Boolean](
      default = true,
      help = "Allow interrupting the Mux client negotiation."
    )

/**
 * Session implementation that attempts to negotiate configuration options with its peer.
 */
private[finagle] final class MuxClientNegotiatingSession(
  handle: PushChannelHandle[ByteReader, Buf],
  version: Short,
  negotiator: Option[Headers] => Future[MuxClientSession],
  headers: Handshake.Headers,
  name: String,
  stats: StatsReceiver)
    extends PushSession[ByteReader, Buf](handle) {

  import MuxClientNegotiatingSession.PushSessionQueue

  private[this] val startNegotiation = new AtomicBoolean(false)
  private[this] val negotiatedSession = Promise[MuxClientSession]()

  if (allowInterruptingClientNegotiation()) {
    // If the session is discarded we tear down and mark the exception
    // as retryable since at this point, clearly nothing was dispatched
    // to the peer.
    negotiatedSession.setInterruptHandler {
      case ex =>
        log.info(
          ex,
          "Mux client negotiation interrupted. "
            + s"Client label: $name, remote address: ${handle.remoteAddress}"
        )
        failHandshake(Failure.retryable(ex))
    }
  }

  // A debug gauge used to track the number of sessions currently negotiating.
  // The utility of this gauge may be short and will likely be removed fast.
  private[this] val negotiatingGauge = stats.addGauge(Verbosity.Debug, "negotiating") {
    if (startNegotiation.get) 1.0f else 0.0f
  }
  negotiatedSession.ensure(negotiatingGauge.remove())

  private[this] val muxHandshakeLatencyStat = stats.stat(Verbosity.Debug, "handshake_latency_us")
  // note, this needs to be volatile since it's set inside the entrypoint `negotiate`
  // which is request driven.
  @volatile private var muxHandshakeStopwatch: () => Duration = null

  private type Phase = Message => Unit

  // Handshaking goes in 'Phase's which encapsulate the state of the handshake.
  // The phases are as follows:
  // 1. Send a marker Rerr message which we expect to be echo'ed if the server
  //    is capable of header exchange.
  // 2. Send our headers (which broadcast information like desired fragment size)
  //    and receive the server headers used to configure the client session.
  //
  // The `phase` field is used to dynamically set the handling behavior for the
  // next message received from the peer.
  // Note: this field should only be modified from within the serial executor
  private[this] var phase: Phase = phaseReceiveMarkerRerr

  /**
   * Perform session negotiation and return a new [[PushSession]] asynchronously.
   */
  def negotiate(): Future[MuxClientSession] = {
    if (startNegotiation.compareAndSet(false, true)) {
      log.debug("Sending Tinit probe to %s", name)
      muxHandshakeStopwatch = Stopwatch.start()
      handle.sendAndForget(Message.encode(MarkerRerr))
    } else {
      log.warning("Attempted to negotiate multiple times with %s", name)
    }

    negotiatedSession
  }

  // If the session fails to negotiate before the handle closes, we need to satisfy the promise
  handle.onClose.respond { reason =>
    val exc = reason match {
      case Return(_) =>
        new ChannelClosedException(handle.remoteAddress).flagged(FailureFlags.Retryable)
      case Throw(t) => t
    }
    failHandshake(exc)
  }

  def onClose: Future[Unit] = handle.onClose

  def close(deadline: Time): Future[Unit] = handle.close(deadline)

  def status: Status = handle.status

  def receive(reader: ByteReader): Unit = {
    try {
      val message = Message.decode(reader)
      if (!startNegotiation.get) {
        log.warning("Received a message from %s before negotiation has started: %s", name, message)
      }

      phase(message)
    } catch {
      case NonFatal(t) =>
        failHandshake(t)
    } finally reader.close()
  }

  private[this] def phaseReceiveMarkerRerr(message: Message): Unit = message match {
    case Message.Rerr(`TinitTag`, `CanTinitMsg`) => // we can negotiate
      phase = phaseReceiveRinit
      if (log.isLoggable(Level.TRACE)) {
        log.trace(s"Server can negotiate; client sending headers $headers")
      }
      handle.sendAndForget(Message.encode(Message.Tinit(TinitTag, version, headers)))

    case _ => // Don't know how to init
      finishNegotiation(None)
  }

  // For when we've received the marker Rerr and are now listening for the servers headers
  private[this] def phaseReceiveRinit(message: Message): Unit = message match {
    case Message.Rinit(_, v, serverHeaders) if v == version =>
      finishNegotiation(Some(serverHeaders))

    case Message.Rerr(_, msg) =>
      failHandshake(Failure(msg))

    case _ =>
      val msg = s"Invalid Tinit response from $name: $message"
      val exc = new IllegalStateException(msg)
      log.warning(exc, msg)
      failHandshake(exc)
  }

  // This must be the only successful pathway forward since we must always yield to the
  // result of `negotiate` even if we don't send+receive any headers from the peer.
  private[this] def finishNegotiation(serverHeaders: Option[Headers]): Unit = {
    log.debug("Init result: %s", serverHeaders)
    // Note, this should never be null because the `negotiate` method is the entry point
    // for the state machine.
    if (muxHandshakeStopwatch != null) {
      muxHandshakeLatencyStat.add(muxHandshakeStopwatch().inMicroseconds)
    }
    // Since this session isn't ready to handle any mux messages,
    // we need to queue them until the `negotiator` is complete.
    // Technically, we shouldn't get any messages in the interim since
    // `negotiator` likely represents tls negotiation, but we do this
    // in case there are any subtle races.
    val q = new PushSessionQueue(handle, stats)
    handle.registerSession(q)
    negotiator(serverHeaders).respond { result =>
      handle.serialExecutor.execute(new Runnable {
        def run(): Unit = result match {
          case Return(clientSession) =>
            if (!negotiatedSession.updateIfEmpty(Return(clientSession))) {
              log.info(
                s"Finished negotiation with $name but handle already closed. "
                  + s"Remote address: ${handle.remoteAddress}"
              )
              q.drainAndClose()
            } else {
              q.drainAndRegister(clientSession)
            }

          case Throw(exc) =>
            val message = s"Mux negotiation failed. Remote address: ${handle.remoteAddress}"
            if (log.isLoggable(Level.DEBUG)) log.debug(exc, message)
            else log.warning(message)
            q.drainAndClose()
            failHandshake(exc)
        }
      })
    }
  }

  // Shortcut for closing the handle and failing the handshake
  private[this] def failHandshake(exc: Throwable): Unit = {
    if (negotiatedSession.updateIfEmpty(Throw(exc))) {
      // only close the handle if we're the one to complete the handshake
      // with a failure, otherwise the handle belongs to someone else.
      handle.close()
    }
  }
}

private[finagle] object MuxClientNegotiatingSession {
  private val log = Logger.get

  val MarkerRerr: Message.Rerr = Message.Rerr(TinitTag, CanTinitMsg)

  /**
   * A [[PushSession]] which queues inbound messages until `drainAndRegister` is called.
   */
  final class PushSessionQueue(handle: PushChannelHandle[ByteReader, Buf], stats: StatsReceiver)
      extends PushSession[ByteReader, Buf](handle) {

    // Based on the usage of this class, we will queue a small amount
    // of elements to close a race window, so we likely don't need to start
    // with a large(r) array.
    private[this] val q = new java.util.ArrayDeque[ByteReader](8)
    private[this] var drained = false
    @volatile private[this] var qsize = 0

    private[this] val qsizeGauge = stats.addGauge(Verbosity.Debug, "negotiating_queue_size") {
      qsize
    }

    // update the state of our variables after 'q' has been drained
    private[this] def markAsDrained(): Unit = {
      qsize = 0
      drained = true
      qsizeGauge.remove() // remove the gauge after we have drained
    }

    /**
     * Drains queued messages into `session` and registers `session`
     * with the handle. Note, this MUST be called from within the `serialExecutor`
     * since it registers a session and passes messages to it via `receive`.
     */
    def drainAndRegister(session: PushSession[ByteReader, Buf]): Unit = {
      val iter = q.iterator()
      while (iter.hasNext) {
        session.receive(iter.next())
        iter.remove()
      }
      handle.registerSession(session)
      markAsDrained()
    }

    /**
     * Removes any queued [[ByteReader]]'s and closes them. Note, this MUST
     * be called from within the `serialExecutor`.
     */
    def drainAndClose(): Unit = {
      val iter = q.iterator()
      while (iter.hasNext) {
        iter.next().close()
        iter.remove()
      }
      markAsDrained()
    }

    def receive(m: ByteReader): Unit = {
      if (!drained) {
        q.add(m)
        qsize = q.size
      } else {
        m.close()
      }
    }

    def status: Status = handle.status
    def onClose: Future[Unit] = handle.onClose
    def close(deadline: Time): Future[Unit] = {
      drainAndClose()
      handle.close(deadline)
    }
  }
}
