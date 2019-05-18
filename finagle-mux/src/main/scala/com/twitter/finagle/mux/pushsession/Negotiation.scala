package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.Mux.param.OppTls
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.transport.{
  IncompatibleNegotiationException,
  MuxFramer,
  OpportunisticTls
}
import com.twitter.finagle.mux.{Handshake, Request, Response}
import com.twitter.finagle.{Service, Stack, param}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future, Promise, Return, Throw, Try}

/**
 * Abstraction of negotiation logic for push-based mux clients and servers
 */
private[finagle] abstract class Negotiation(
  params: Stack.Params,
  sharedStats: SharedNegotiationStats) {

  type SessionT <: PushSession[ByteReader, Buf]

  private[this] val log = Logger.get

  protected def builder(
    handle: PushChannelHandle[ByteReader, Buf],
    writer: MessageWriter,
    decoder: MuxMessageDecoder
  ): SessionT

  private[this] def remoteAddressString(handle: PushChannelHandle[_, _]): String =
    s"remote: ${handle.remoteAddress}"

  // effectual method that may throw
  private[this] def negotiateOppTls(
    handle: PushChannelHandle[ByteReader, Buf],
    peerHeaders: Option[Headers],
    onTlsHandshakeComplete: Try[Unit] => Unit
  ): Unit = {

    def turnOnTls(): Unit = handle match {
      case h: MuxChannelHandle => h.turnOnTls(onTlsHandshakeComplete)
      case other =>
        // Should never happen when building a true client
        throw new IllegalStateException(
          "Expected to find a MuxChannelHandle, instead found " +
            s"$other. Couldn't turn on TLS. ${remoteAddressString(handle)}"
        )
    }

    val localEncryptLevel = params[OppTls].level.getOrElse(OpportunisticTls.Off)
    val remoteEncryptLevel = peerHeaders
      .flatMap(Handshake.valueOf(OpportunisticTls.Header.KeyBuf, _)) match {
      case Some(buf) => OpportunisticTls.Header.decodeLevel(buf)
      case None =>
        log.debug(
          "Peer either didn't negotiate or didn't send an Opportunistic Tls preference: " +
            s"defaulting to remote encryption level of Off. ${remoteAddressString(handle)}"
        )
        OpportunisticTls.Off
    }

    try {
      val useTls = OpportunisticTls.negotiate(localEncryptLevel, remoteEncryptLevel)
      if (log.isLoggable(Level.DEBUG)) {
        log.debug(
          s"Successfully negotiated TLS with remote peer. Using TLS: $useTls local level: " +
            s"$localEncryptLevel, remote level: $remoteEncryptLevel. ${remoteAddressString(handle)}"
        )
      }
      if (useTls) {
        sharedStats.tlsSuccess.incr()
        turnOnTls()
      } else {
        // synthesize a handshake complete for `negotiateAsync`
        onTlsHandshakeComplete(Return.Unit)
      }
    } catch {
      case exn: IncompatibleNegotiationException =>
        sharedStats.tlsFailures.incr()
        log.fatal(
          exn,
          s"The local peer wanted $localEncryptLevel and the remote peer wanted" +
            s" $remoteEncryptLevel which are incompatible. ${remoteAddressString(handle)}"
        )
        throw exn
    }
  }

  /**
   * Attempt to negotiate the final Mux session.
   *
   * @note If negotiation fails an appropriate exception may be thrown.
   */
  private[this] def negotiateMuxSession(
    handle: PushChannelHandle[ByteReader, Buf],
    peerHeaders: Option[Headers]
  ): SessionT = {
    val writeManager = {
      val fragmentSize = peerHeaders
        .flatMap(Handshake.valueOf(MuxFramer.Header.KeyBuf, _))
        .map(MuxFramer.Header.decodeFrameSize(_))
        .getOrElse(Int.MaxValue)
      new FragmentingMessageWriter(handle, fragmentSize, sharedStats)
    }
    val messageDecoder = new FragmentDecoder(sharedStats)

    builder(handle, writeManager, messageDecoder)
  }

  /**
   * Returns a Mux session with TLS negotiated synchronously (i.e. before
   * TLS handshaking is complete).
   */
  final def negotiate(
    handle: PushChannelHandle[ByteReader, Buf],
    peerHeaders: Option[Headers]
  ): SessionT = {
    val onHandshakeComplete: Try[Unit] => Unit = {
      case Return(_) =>
        log.trace("Successfully negotiated oppTls handshake")
      case Throw(t) =>
        log.trace("OppTls handshake failed")
    }

    if (log.isLoggable(Level.TRACE)) {
      negotiateOppTls(handle, peerHeaders, onHandshakeComplete)
    } else {
      negotiateOppTls(handle, peerHeaders, _ => ())
    }
    negotiateMuxSession(handle, peerHeaders)
  }

  /**
   * Returns a Future Mux session which is satisfied when TLS handshaking
   * is complete.
   */
  final def negotiateAsync(
    handle: PushChannelHandle[ByteReader, Buf],
    peerHeaders: Option[Headers]
  ): Future[SessionT] = {
    // Note, we don't set an interrupt handler on this promise since
    // there is no meaningful way to gracefully interrupt a tls handshake.
    // Instead, the expectation is that users of this method should handle
    // interrupts by closing the underlying channel. This is what
    // `MuxClientNegotiatingSession` does.
    val p = Promise[Unit]
    val onHandshakeComplete: Try[Unit] => Unit = { result =>
      result match {
        case Return(_) => {
          if (log.isLoggable(Level.TRACE))
            log.trace("Client side successfully negotiated oppTls handshake")
          p.setDone
        }

        case Throw(t) => {
          if (log.isLoggable(Level.TRACE))
            log.trace("Client side failed to negotiate oppTls handshake")
          p.setException(t)
        }
      }
    }
    Try(negotiateOppTls(handle, peerHeaders, onHandshakeComplete)) match {
      case Return(_) => p.map(_ => negotiateMuxSession(handle, peerHeaders))
      case Throw(t) => Future.exception(t)
    }
  }
}

private[finagle] object Negotiation {
  final class Client(params: Stack.Params, sharedStats: SharedNegotiationStats)
      extends Negotiation(params, sharedStats) {

    override type SessionT = MuxClientSession

    protected def builder(
      handle: PushChannelHandle[ByteReader, Buf],
      writer: MessageWriter,
      decoder: MuxMessageDecoder
    ): MuxClientSession = {
      new MuxClientSession(
        handle,
        decoder,
        writer,
        params[FailureDetector.Param].param,
        params[param.Label].label,
        params[param.Stats].statsReceiver,
        params[param.Timer].timer
      )
    }
  }

  final class Server(
    params: Stack.Params,
    sharedStats: SharedNegotiationStats,
    service: Service[Request, Response])
      extends Negotiation(params, sharedStats) {

    override type SessionT = MuxServerSession

    protected def builder(
      handle: PushChannelHandle[ByteReader, Buf],
      writer: MessageWriter,
      decoder: MuxMessageDecoder
    ): MuxServerSession = {
      new MuxServerSession(
        params,
        decoder,
        writer,
        handle,
        service
      )
    }
  }
}
