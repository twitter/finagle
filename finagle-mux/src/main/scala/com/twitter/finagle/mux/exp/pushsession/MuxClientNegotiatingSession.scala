package com.twitter.finagle.mux.exp.pushsession

import com.twitter.app.GlobalFlag
import com.twitter.finagle.{ChannelClosedException, Failure, FailureFlags, Status}
import com.twitter.finagle.mux.Handshake.{CanTinitMsg, Headers, TinitTag}
import com.twitter.finagle.mux.exp.pushsession.MuxClientNegotiatingSession._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.mux.Handshake
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/**
 * Allow interrupting mux client negotiation. This is behind a flag because we've seen
 * some ill effects in production. This is related to session acquisition being the burden
 * of the first request of the session and interrupting a single request being able to
 * abort session acquisition.
 *
 * @note flag is temporary so we can easily test it. After we resolve the production
 *       issues this will become enabled by default, and soon thereafter the flag will
 *       be removed.
 */
private object allowInterruptingClientNegotiation extends GlobalFlag[Boolean](
  default = false,
  help = "Allow interrupting the Mux client negotiation.")

/**
 * Session implementation that attempts to negotiate configuration options with its peer.
 */
private[finagle] final class MuxClientNegotiatingSession(
  handle: PushChannelHandle[ByteReader, Buf],
  version: Short,
  negotiator: Option[Headers] => MuxClientSession,
  headers: Handshake.Headers,
  name: String,
  stats: StatsReceiver
) extends PushSession[ByteReader, Buf](handle) {

  private[this] val startNegotiation = new AtomicBoolean(false)
  private[this] val negotiatedSession = Promise[MuxClientSession]()

  if (allowInterruptingClientNegotiation()) {
    // If the session is discarded we tear down and mark the exception
    // as retryable since at this point, clearly nothing was dispatched
    // to the peer.
    negotiatedSession.setInterruptHandler {
      case ex =>
        log.info(ex, "Mux client negotiation interrupted.")
        failHandshake(Failure.retryable(ex))
    }
  }

  // A debug gauge used to track the number of sessions currently negotiating.
  // The utility of this gauge may be short and will likely be removed fast.
  private[this] val negotiatingGauge = stats.addGauge(Verbosity.Debug, "negotiating") {
    if (startNegotiation.get) 1.0f else 0.0f
  }
  negotiatedSession.ensure(negotiatingGauge.remove())

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
        log.warning(
          "Received a message from %s before negotiation has started: %s", name, message)
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
    Try(negotiator(serverHeaders)) match {
      case Return(clientSession) =>
        handle.registerSession(clientSession)
        if (!negotiatedSession.updateIfEmpty(Return(clientSession))) {
          log.debug("Finished negotiation with %s but handle already closed.", name)
        }

      case Throw(exc) =>
        log.warning(exc, "Mux negotiation failed.")
        failHandshake(exc)
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
}
