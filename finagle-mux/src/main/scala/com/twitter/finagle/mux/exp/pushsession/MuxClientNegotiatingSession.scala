package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.{ChannelClosedException, Failure, Mux, Status}
import com.twitter.finagle.mux.Handshake.{CanTinitMsg, Headers, TinitTag}
import com.twitter.finagle.mux.exp.pushsession.MuxClientNegotiatingSession._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushSession}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/**
 * Session implementation that attempts to negotiate configuration options with its peer.
 */
private[finagle] final class MuxClientNegotiatingSession(
  handle: PushChannelHandle[ByteReader, Buf],
  version: Short,
  negotiator: Option[Headers] => MuxClientSession,
  maxFrameSize: StorageUnit,
  name: String
) extends PushSession[ByteReader, Buf](handle) {

  private[this] val negotiatedSession = Promise[MuxClientSession]()
  private[this] val startNegotiation = new AtomicBoolean(false)

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
      case Return(_) => new ChannelClosedException(handle.remoteAddress)
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
      // TODO: need to support OptTls
      val headers = Mux.Client.headers(maxFrameSize, None)
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

  private[this] def finishNegotiation(serverHeaders: Option[Headers]): Unit = {
    log.debug("Init result: %s", serverHeaders)
    val clientSession = negotiator(serverHeaders)
    handle.registerSession(clientSession)
    if (!negotiatedSession.updateIfEmpty(Return(clientSession))) {
      log.debug("Finished negotiation with %s but handle already closed.", name)
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

private object MuxClientNegotiatingSession {
  private val log = Logger.get

  private val MarkerRerr: Message.Rerr = Message.Rerr(TinitTag, CanTinitMsg)
}
