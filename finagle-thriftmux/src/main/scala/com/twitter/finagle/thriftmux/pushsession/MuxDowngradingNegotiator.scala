package com.twitter.finagle.thriftmux.pushsession

import com.twitter.finagle.mux.pushsession.{
  MessageWriter,
  MuxChannelHandle,
  MuxMessageDecoder,
  Negotiation,
  SharedNegotiationStats
}
import com.twitter.finagle._
import com.twitter.finagle.mux.{Request, Response}
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.transport.{BadMessageException, Message}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession, RefPushSession}
import com.twitter.finagle.thrift.thrift.{ResponseHeader, UpgradeReply}
import com.twitter.finagle.thrift.{InputBuffer, OutputBuffer, ThriftTracing}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.Logger
import com.twitter.util._
import org.apache.thrift.protocol.{TMessage, TMessageType, TProtocolFactory}
import scala.util.control.NonFatal

/**
 * Check the first message and check if its a valid mux message, and if not,
 * downgrade to the vanilla thrift protocol.
 *
 * We indirect through the `RefPushSession` because the server needs a handle to close
 * and that handle needs to be able to target a mux session which is capable of doing
 * deliberate draining. Since we don't start life with that level of ability, we need
 * to use the `RefPushSession` to allow us to update the target of close calls.
 */
// The server negotiating session is driving itself and nobody needs a handle on it
private[finagle] final class MuxDowngradingNegotiator(
  refSession: RefPushSession[ByteReader, Buf],
  params: Stack.Params,
  sharedStats: SharedNegotiationStats,
  handle: MuxChannelHandle,
  service: Service[Request, Response])
    extends PushSession[ByteReader, Buf](handle) {
  import MuxDowngradingNegotiator._

  // This should only be satisfied after we've completed an upgrade, or failed
  // due to an unrecognized protocol or TLS requirements.
  private[this] val handshakeDone = Promise[Unit]

  private[this] val sr = params[Stats].statsReceiver
  private[this] val thriftmuxConnects = sr.counter("thriftmux", "connects")
  private[this] val downgradedConnects = sr.counter("thriftmux", "downgraded_connects")

  // If the handle closes before we finish negotiation we need to shutdown
  handle.onClose.ensure {
    if (!handshakeDone.isDefined) close()
  }

  def close(deadline: Time): Future[Unit] = {
    // We want to proxy close calls to the underlying session, provided it resolves in time.
    // This facilitates draining behavior.
    handshakeDone.by(deadline)(params[param.Timer].timer).transform {
      case Return(_) => refSession.close(deadline)
      case Throw(t) => closeWithException(t)
    }
  }

  private[this] def closeWithException(t: Throwable): Future[Unit] = {
    val f = Closable.all(handle, service).close()
    // We shouldn't have to `updateIfEmpty`, but we do just in case.
    handshakeDone.updateIfEmpty(Throw(t))
    f
  }

  def receive(reader: ByteReader): Unit = {
    try {
      val buf = reader.readAll()
      checkDowngrade(buf)
    } catch {
      case NonFatal(ex) =>
        log.error(ex, "Uncaught exception during mux downgrade negotiation. Closing session.")
        closeWithException(ex)
    } finally reader.close()
  }

  def status: Status = handle.status

  private[this] def checkDowngrade(buf: Buf): Unit = {
    Try { Message.decode(buf) } match {
      // We assume that a bad message decode indicates a thrift
      // session. Due to Mux message numbering, a binary-encoded
      // thrift frame corresponds to an Rerr message with tag
      // 65537. Note that in this context, an R-message is never
      // valid.
      //
      // Binary-encoded thrift messages have the format
      //
      //     header:4 n:4 method:n seqid:4
      //
      // The header is
      //
      //     0x80010000 | type
      //
      // where the type of CALL is 1; the type of ONEWAY is 4. This makes
      // the first four bytes of a CALL message 0x80010001.
      //
      // Mux messages begin with
      //
      //     Type:1 tag:3
      //
      // Rerr is type 0x80, so we see the above thrift header
      // Rerr corresponds to (tag=0x010001).
      //
      // The hazards of protocol multiplexing.
      case Throw(Failure(Some(_: BadMessageException))) | Return(Message.Rerr(65537, _)) | Return(
            Message.Rerr(65540, _)) =>
        initThriftDowngrade(buf)

      // We have a valid mux session
      case Return(_) =>
        initThriftMux(buf)

      case Throw(exc) =>
        val msg = s"Unable to determine the protocol. $remoteAddressString"
        log.info(exc, msg)
        closeWithException(exc)
    }
  }

  private[this] def initThriftMux(buf: Buf): Unit = {
    thriftmuxConnects.incr()
    // We have a normal mux transport! Just install the handshaker, give it this
    // first message, and be on our way!
    Mux.Server.defaultSessionFactory(refSession, params, sharedStats, handle, service)
    refSession.receive(ByteReader(buf))
    handshakeDone.setDone()
  }

  private[this] def initThriftDowngrade(buf: Buf): Unit = {
    downgradedConnects.incr()
    val protocolFactory = params[Thrift.param.ProtocolFactory].protocolFactory
    val isTTwitter = checkTTwitter(protocolFactory, buf)

    val ttwitterHeader =
      if (!isTTwitter) None
      else
        Some {
          Buf.ByteArray.Owned(OutputBuffer.messageToArray(new ResponseHeader, protocolFactory))
        }

    // We install our new session and then send it the first thrift dispatch
    try {
      val nextSession =
        new DowngradeNegotiator(ttwitterHeader, params, sharedStats, service)
          .negotiate(handle, None)
      // Register the new session and then give it the message
      refSession.updateRef(nextSession)

      // If we're TTwitter, the first message was an init and we need to ack it.
      // If we're not TTwitter, the first message was a dispatch and needs to be handled.
      if (!isTTwitter) refSession.receive(ByteReader(buf))
      else {
        handle.sendAndForget {
          val buffer = new OutputBuffer(protocolFactory)
          buffer().writeMessageBegin(
            new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.REPLY, 0)
          )
          val upgradeReply = new UpgradeReply
          upgradeReply.write(buffer())
          buffer().writeMessageEnd()
          Buf.ByteArray.Shared(buffer.toArray)
        }
      }
      handshakeDone.setDone()
    } catch {
      case NonFatal(t) =>
        // Negotiation failed, so we need to cleanup and shutdown.
        log.warning(t, s"Negotiation failed. Closing session. $remoteAddressString")
        closeWithException(t)
    }
  }

  private[this] def checkTTwitter(protocolFactory: TProtocolFactory, buf: Buf): Boolean =
    try {
      val buffer = new InputBuffer(Buf.ByteArray.Owned.extract(buf), protocolFactory)
      val msg = buffer().readMessageBegin()
      msg.`type` == TMessageType.CALL &&
      msg.name == ThriftTracing.CanTraceMethodName
    } catch {
      case NonFatal(_) => false
    }

  private[this] def remoteAddressString: String = s"Remote: ${handle.remoteAddress}"
}

private[finagle] object MuxDowngradingNegotiator {
  private val log = Logger.get

  private final class DowngradeNegotiator(
    ttwitterHeader: Option[Buf],
    params: Stack.Params,
    sharedStats: SharedNegotiationStats,
    service: Service[Request, Response])
      extends Negotiation(params, sharedStats) {

    override type SessionT = VanillaThriftSession

    protected def negotiateCompression(
      handle: PushChannelHandle[ByteReader, Buf],
      peerHeaders: Option[Headers]
    ): Unit = ()

    override protected def builder(
      handle: PushChannelHandle[ByteReader, Buf],
      writer: MessageWriter,
      decoder: MuxMessageDecoder
    ): VanillaThriftSession = {
      new VanillaThriftSession(handle, ttwitterHeader, params, service)
    }
  }

  def build(
    ref: RefPushSession[ByteReader, Buf],
    params: Stack.Params,
    sharedStats: SharedNegotiationStats,
    handle: MuxChannelHandle,
    service: Service[Request, Response]
  ): ref.type = {
    val negotiatingSession = new MuxDowngradingNegotiator(
      refSession = ref,
      params = params,
      sharedStats = sharedStats,
      handle = handle,
      service = service
    )

    ref.updateRef(negotiatingSession)
    ref
  }
}
