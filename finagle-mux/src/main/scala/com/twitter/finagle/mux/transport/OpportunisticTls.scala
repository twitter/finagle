package com.twitter.finagle.mux.transport

import com.twitter.finagle.{FailureFlags, Mux, SourcedException, Stack}
import com.twitter.finagle.transport.{Transport, ContextBasedTransport}
import com.twitter.io.Buf
import com.twitter.logging.{Logger, HasLogLevel, Level}
import com.twitter.util.Future
import io.netty.channel.Channel
import scala.util.control.NoStackTrace

private[twitter] object OpportunisticTls {
  private[this] val log = Logger.get()

  /**
   * Defines encrypter keys and values exchanged as part of a
   * mux session header during initialization.
   */
  private[finagle] object Header {
    val KeyBuf: Buf = Buf.Utf8("tls")

    /**
     * Extracts level from the `buf`.
     */
    def decodeLevel(buf: Buf): Level =
      if (buf == Off.buf) Off
      else if (buf == Desired.buf) Desired
      else if (buf == Required.buf) Required
      else {
        val Buf.Utf8(bad) = buf
        log.debug(s"Expected one of 'off', 'desired', or 'required' but received $bad")
        Off // don't want to fail in case we decide to change levels in the future.
      }
  }

  /**
   * Negotiates what the negotiated agreement is.
   *
   * Returns true if the client and server agreed to use tls, false if they
   * agreed not to.
   *
   * Throws an IncompatibleNegotiationException if the negotiation failed.
   */
  private[finagle] def negotiate(left: Level, right: Level): Boolean = (left, right) match {
    case (Off, Off) => false
    case (Off, Desired) => false
    case (Off, Required) => throw new IncompatibleNegotiationException
    case (Desired, Off) => false
    case (Desired, Desired) => true
    case (Desired, Required) => true
    case (Required, Off) => throw new IncompatibleNegotiationException
    case (Required, Desired) => true
    case (Required, Required) => true
  }

  /**
   * Wraps the underlying transport, adding a way to turn on tls for clients.
   */
  private[finagle] def transport(
    ch: Channel,
    params: Stack.Params,
    transport: Transport[Any, Any]
  ): Transport[Any, Any] { type Context = MuxContext } = {
    new ContextBasedTransport[Any, Any, MuxContext](
      new MuxContext(transport.context, () => params[Mux.param.TurnOnTlsFn].fn(params, ch.pipeline))
    ) {
      def read(): Future[Any] = transport.read()
      def write(any: Any): Future[Unit] = transport.write(any)
    }
  }

  /**
   * Configures the level of TLS that the client or server can support or must
   * support.
   */
  sealed abstract class Level(value: String) {
    val buf: Buf = Buf.Utf8(value)
  }

  /**
   * Indicates that the peer cannot upgrade to tls.
   *
   * Compatible with "off", or "desired".
   */
  case object Off extends Level("off")

  /**
   * Indicates that the peer can upgrade to tls.
   *
   * The peer will upgrade to tls if the remote peer is "desired", or
   * "required", and will stay on cleartext if the remote peer is "off".
   * Compatible with "off", "desired", or "required".
   */
  case object Desired extends Level("desired")

  /**
   * Indicates that the peer must upgrade to tls.
   *
   * Compatible with "desired", or "required".
   */
  case object Required extends Level("required")
}

/**
 * Unable to negotiate whether to use TLS or not with the remote peer.
 *
 * This means that one party indicated that it required encryption, and the
 * other party either indicated that it did not support encryption, or it
 * didn't support negotiating encryption at all.
 */
class IncompatibleNegotiationException(
  private[finagle] val flags: Long = FailureFlags.Empty
) extends Exception("Could not negotiate whether to use TLS or not.")
    with FailureFlags[IncompatibleNegotiationException]
    with HasLogLevel
    with SourcedException
    with NoStackTrace {
  def logLevel: Level = Level.ERROR
  protected def copyWithFlags(flags: Long): IncompatibleNegotiationException =
    new IncompatibleNegotiationException(flags)
}
