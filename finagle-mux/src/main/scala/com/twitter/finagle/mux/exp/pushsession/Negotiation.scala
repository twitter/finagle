package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.Mux.param.OppTls
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.{Handshake, Request, Response}
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.transport.{IncompatibleNegotiationException, MuxFramer, OpportunisticTls}
import com.twitter.finagle.{Service, Stack, param}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.{Level, Logger}

/**
 * Abstraction of negotiation logic for push-based mux clients and servers
 */
private[finagle] abstract class Negotiation(params: Stack.Params) {

  type SessionT <: PushSession[ByteReader, Buf]

  private[this] val log = Logger.get
  private[this] val statsReceiver = params[param.Stats].statsReceiver

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
    peerHeaders: Option[Headers]
  ): Unit = {

    def turnOnTls(): Unit = handle match {
      case h: MuxChannelHandle => h.turnOnTls()
      case other =>
        // Should never happen when building a true client
        throw new IllegalStateException(
          "Expected to find a MuxChannelHandle, instead found " +
            s"$other. Couldn't turn on TLS. ${remoteAddressString(handle)}")
    }

    val localEncryptLevel = params[OppTls].level.getOrElse(OpportunisticTls.Off)
    val remoteEncryptLevel = peerHeaders
      .flatMap(Handshake.valueOf(OpportunisticTls.Header.KeyBuf, _)) match {
      case Some(buf) => OpportunisticTls.Header.decodeLevel(buf)
      case None =>
        log.debug("Peer either didn't negotiate or didn't send an Opportunistic Tls preference: " +
          s"defaulting to remote encryption level of Off. ${remoteAddressString(handle)}")
        OpportunisticTls.Off
    }

    try {
      val useTls = OpportunisticTls.negotiate(localEncryptLevel, remoteEncryptLevel)
      if (log.isLoggable(Level.DEBUG)) {
        log.debug(s"Successfully negotiated TLS with remote peer. Using TLS: $useTls local level: " +
          s"$localEncryptLevel, remote level: $remoteEncryptLevel. ${remoteAddressString(handle)}")
      }
      if (useTls) {
        statsReceiver.counter("tls", "upgrade", "success").incr()
        turnOnTls()
      }
    } catch {
      case exn: IncompatibleNegotiationException =>
        statsReceiver.counter("tls", "upgrade", "incompatible").incr()
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
  final def negotiate(
    handle: PushChannelHandle[ByteReader, Buf],
    peerHeaders: Option[Headers]
  ): SessionT = {
    negotiateOppTls(handle, peerHeaders)
    val framingStats = statsReceiver.scope("framer")
    val writeManager = {
      val fragmentSize = peerHeaders
        .flatMap(Handshake.valueOf(MuxFramer.Header.KeyBuf, _))
        .map(MuxFramer.Header.decodeFrameSize(_))
        .getOrElse(Int.MaxValue)
      new FragmentingMessageWriter(handle, fragmentSize, framingStats)
    }
    val messageDecoder = new FragmentDecoder(framingStats)

    builder(handle, writeManager, messageDecoder)
  }
}

private[finagle] object Negotiation {
  final class Client(params: Stack.Params) extends Negotiation(params) {
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

  final class Server(params: Stack.Params, service: Service[Request, Response])
    extends Negotiation(params) {
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
