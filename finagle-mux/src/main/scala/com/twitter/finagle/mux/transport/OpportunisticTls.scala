package com.twitter.finagle.mux.transport

import com.twitter.finagle.ssl.{OpportunisticTls => SslOptTls}
import com.twitter.finagle.{FailureFlags, SourcedException}
import com.twitter.io.Buf
import com.twitter.logging.{HasLogLevel, Level => LogLevel, Logger}
import scala.util.control.NoStackTrace

private[twitter] object OpportunisticTls {
  private[this] val log = Logger.get()

  // Type and reference aliases to make the migration easier.
  @Deprecated
  type Level = SslOptTls.Level

  @Deprecated
  type Off = SslOptTls.Off.type

  @Deprecated
  type Required = SslOptTls.Required.type

  @Deprecated
  type Desired = SslOptTls.Desired.type

  @Deprecated
  val Off: Off = SslOptTls.Off

  @Deprecated
  val Required: Required = SslOptTls.Required

  @Deprecated
  val Desired: Desired = SslOptTls.Desired

  /** The sequence of [[SslOptTls.Level]]s from least to most secure */
  @deprecated("Please use SslOptTls.Values directly", "2021-03-11")
  final def Values: Seq[SslOptTls.Level] = SslOptTls.Values

  /**
   * Defines encrypter keys and values exchanged as part of a
   * mux session header during initialization.
   */
  private[finagle] object Header {
    val KeyBuf: Buf = Buf.Utf8("tls")

    /**
     * Extracts level from the `buf`.
     */
    def decodeLevel(buf: Buf): SslOptTls.Level =
      if (buf == SslOptTls.Off.buf) SslOptTls.Off
      else if (buf == SslOptTls.Desired.buf) SslOptTls.Desired
      else if (buf == SslOptTls.Required.buf) SslOptTls.Required
      else {
        val Buf.Utf8(bad) = buf
        log.debug(s"Expected one of 'off', 'desired', or 'required' but received $bad")
        SslOptTls.Off // don't want to fail in case we decide to change levels in the future.
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
  private[finagle] def negotiate(left: SslOptTls.Level, right: SslOptTls.Level): Boolean =
    (left, right) match {
      case (SslOptTls.Off, SslOptTls.Off) => false
      case (SslOptTls.Off, SslOptTls.Desired) => false
      case (SslOptTls.Off, SslOptTls.Required) => throw new IncompatibleNegotiationException
      case (SslOptTls.Desired, SslOptTls.Off) => false
      case (SslOptTls.Desired, SslOptTls.Desired) => true
      case (SslOptTls.Desired, SslOptTls.Required) => true
      case (SslOptTls.Required, SslOptTls.Off) => throw new IncompatibleNegotiationException
      case (SslOptTls.Required, SslOptTls.Desired) => true
      case (SslOptTls.Required, SslOptTls.Required) => true
    }
}

/**
 * Unable to negotiate whether to use TLS or not with the remote peer.
 *
 * This means that one party indicated that it required encryption, and the
 * other party either indicated that it did not support encryption, or it
 * didn't support negotiating encryption at all.
 */
class IncompatibleNegotiationException(val flags: Long = FailureFlags.Empty)
    extends Exception("Could not negotiate whether to use TLS or not.")
    with FailureFlags[IncompatibleNegotiationException]
    with HasLogLevel
    with SourcedException
    with NoStackTrace {
  def logLevel: LogLevel = LogLevel.ERROR
  protected def copyWithFlags(flags: Long): IncompatibleNegotiationException =
    new IncompatibleNegotiationException(flags)
}
