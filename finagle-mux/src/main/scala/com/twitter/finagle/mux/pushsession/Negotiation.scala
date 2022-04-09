package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.Mux.param.CompressionPreferences
import com.twitter.finagle.pushsession.PushChannelHandle
import com.twitter.finagle.pushsession.PushSession
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.transport.Compression
import com.twitter.finagle.mux.transport.CompressionNegotiation
import com.twitter.finagle.mux.transport.IncompatibleNegotiationException
import com.twitter.finagle.mux.transport.MuxFramer
import com.twitter.finagle.mux.transport.{OpportunisticTls => MuxOpportunisticTls}
import com.twitter.finagle.mux.Handshake
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.Response
import com.twitter.finagle.param.Label
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.ssl.OpportunisticTls
import com.twitter.finagle.Service
import com.twitter.finagle.Stack
import com.twitter.finagle.param
import com.twitter.io.Buf
import com.twitter.io.ByteReader
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

/**
 * Abstraction of negotiation logic for push-based mux clients and servers
 */
private[finagle] abstract class Negotiation(
  params: Stack.Params,
  sharedStats: SharedNegotiationStats,
  isServer: Boolean) {

  type SessionT <: PushSession[ByteReader, Buf]

  private[this] val log = Logger.get

  private[this] val myName: String = {
    val label = params[Label].label
    if (isServer) s"Server($label)"
    else s"Client($label)"
  }

  /**
   * Negotiates which compression formats will be used for the request and
   * response streams.
   */
  protected def negotiateCompression(
    handle: PushChannelHandle[ByteReader, Buf],
    peerHeaders: Option[Headers]
  ): Unit

  protected def builder(
    handle: PushChannelHandle[ByteReader, Buf],
    writer: MessageWriter,
    decoder: MuxMessageDecoder
  ): SessionT

  final protected def asNegotiatingHandle(
    handle: PushChannelHandle[ByteReader, Buf],
    feature: String
  ): NegotiatingHandle =
    handle match {
      case h: NegotiatingHandle => h
      case other =>
        // Should never happen when building a true client
        throw new IllegalStateException(
          "Expected to find a MuxChannelHandle, instead found " +
            s"$other. Couldn't turn on $feature. ${remoteAddressString(handle)}"
        )
    }

  private[this] def remoteAddressString(handle: PushChannelHandle[_, _]): String =
    s"remote: ${handle.remoteAddress}"

  // effectual method that may throw
  private[this] def negotiateOppTls(
    handle: PushChannelHandle[ByteReader, Buf],
    peerHeaders: Option[Headers],
    onTlsHandshakeComplete: Try[Unit] => Unit
  ): Unit =
    if (handle.sslSessionInfo.usingSsl) {
      // If we're already encrypted this is already decided and we're at max security.
      log.debug(s"$myName: Session already encrypted. Skipping OppTls header check.")
      onTlsHandshakeComplete(Try.Unit)
    } else {
      negotiateOppTlsViaHeaders(handle, peerHeaders, onTlsHandshakeComplete)
    }

  private[this] def negotiateOppTlsViaHeaders(
    handle: PushChannelHandle[ByteReader, Buf],
    peerHeaders: Option[Headers],
    onTlsHandshakeComplete: Try[Unit] => Unit
  ): Unit = {
    val localEncryptLevel = params[OppTls].level.getOrElse(OpportunisticTls.Off)
    val remoteEncryptLevel = peerHeaders
      .flatMap(Handshake.valueOf(MuxOpportunisticTls.Header.KeyBuf, _)) match {
      case Some(buf) => MuxOpportunisticTls.Header.decodeLevel(buf)
      case None =>
        log.debug(
          s"$myName: " +
            "Peer either didn't negotiate or didn't send an Opportunistic Tls preference: " +
            s"defaulting to remote encryption level of Off. ${remoteAddressString(handle)}")
        OpportunisticTls.Off
    }

    try {
      val useTls = MuxOpportunisticTls.negotiate(localEncryptLevel, remoteEncryptLevel)
      if (log.isLoggable(Level.DEBUG)) {
        log.debug(
          s"$myName: Successfully negotiated TLS with remote peer. " +
            s"Using TLS: $useTls local level: " +
            s"$localEncryptLevel, remote level: $remoteEncryptLevel. ${remoteAddressString(handle)}"
        )
      }
      if (useTls) {
        sharedStats.tlsSuccess.incr()
        asNegotiatingHandle(handle, "TLS").turnOnTls(onTlsHandshakeComplete)
      } else {
        // synthesize a handshake complete for `negotiateAsync`
        onTlsHandshakeComplete(Return.Unit)
      }
    } catch {
      case exn: IncompatibleNegotiationException =>
        sharedStats.tlsFailures.incr()
        log.fatal(
          exn,
          s"$myName: The local peer wanted $localEncryptLevel and the remote peer wanted" +
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
    negotiateOppTls(
      handle,
      peerHeaders,
      _ match {
        case _ if !log.isLoggable(Level.TRACE) => // nop, no logging
        case Return(_) =>
          log.trace(s"$myName: Successfully negotiated oppTls handshake")
        case Throw(t) =>
          log.trace(t, s"$myName: OppTls handshake failed")
      }
    )

    negotiateCompression(handle, peerHeaders)
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
    val onHandshakeComplete: Try[Unit] => Unit = _ match {
      case Return(_) =>
        if (log.isLoggable(Level.TRACE))
          log.trace(s"$myName: successfully negotiated oppTls handshake")
        p.setDone

      case Throw(t) =>
        if (log.isLoggable(Level.TRACE))
          log.trace(t, s"$myName: side failed to negotiate oppTls handshake")
        p.setException(t)
    }

    Try(negotiateOppTls(handle, peerHeaders, onHandshakeComplete)) match {
      case Return(_) =>
        p.map { _ =>
          negotiateCompression(handle, peerHeaders)
          negotiateMuxSession(handle, peerHeaders)
        }
      case Throw(t) => Future.exception(t)
    }
  }
}

private[finagle] object Negotiation {

  final class Client(params: Stack.Params, sharedStats: SharedNegotiationStats)
      extends Negotiation(params, sharedStats, isServer = false) {

    override type SessionT = MuxClientSession

    protected def negotiateCompression(
      handle: PushChannelHandle[ByteReader, Buf],
      peerHeaders: Option[Headers]
    ): Unit = {
      val compressionFormats = peerHeaders
        .flatMap(Handshake.valueOf(CompressionNegotiation.ServerHeader.KeyBuf, _))
        .map(CompressionNegotiation.ServerHeader.decode(_))
        .getOrElse(CompressionNegotiation.CompressionOff)

      compressionFormats.request match {
        case Some(compression) =>
          sharedStats.compressionSuccess.incr()
          val negotiatingHandle = asNegotiatingHandle(handle, "compression")
          negotiatingHandle.turnOnCompression(compression)
        case _ =>
          sharedStats.compressionFailures.incr()
      }
      compressionFormats.response match {
        case Some(decompression) =>
          sharedStats.decompressionSuccess.incr()
          val negotiatingHandle = asNegotiatingHandle(handle, "decompression")
          negotiatingHandle.turnOnDecompression(decompression)
        case _ =>
          sharedStats.decompressionFailures.incr()
      }
    }

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
        params[param.Stats].statsReceiver
      )
    }
  }

  final class Server(
    params: Stack.Params,
    sharedStats: SharedNegotiationStats,
    service: Service[Request, Response])
      extends Negotiation(params, sharedStats, isServer = true) {

    override type SessionT = MuxServerSession

    protected def negotiateCompression(
      handle: PushChannelHandle[ByteReader, Buf],
      peerHeaders: Option[Headers]
    ): Unit = {
      val peerCompressionSettings = peerHeaders
        .flatMap(Handshake.valueOf(CompressionNegotiation.ClientHeader.KeyBuf, _))
        .map(CompressionNegotiation.ClientHeader.decode(_))
        .getOrElse(Compression.PeerCompressionOff)

      val ourCompressionSettings = params[CompressionPreferences].compressionPreferences

      val compressionFormats =
        CompressionNegotiation.negotiate(ourCompressionSettings, peerCompressionSettings)

      compressionFormats.request match {
        case Some(decompression) =>
          sharedStats.decompressionSuccess.incr()
          val negotiatingHandle = asNegotiatingHandle(handle, "decompression")
          negotiatingHandle.turnOnDecompression(decompression)
        case _ =>
          sharedStats.decompressionFailures.incr()
      }
      compressionFormats.response match {
        case Some(compression) =>
          sharedStats.compressionSuccess.incr()
          val negotiatingHandle = asNegotiatingHandle(handle, "compression")
          negotiatingHandle.turnOnCompression(compression)
        case _ =>
          sharedStats.compressionFailures.incr()
      }
    }

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
