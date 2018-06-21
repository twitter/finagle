package com.twitter.finagle.mux.transport

import com.twitter.finagle.FailureFlags
import com.twitter.io.{Buf, BufByteWriter, ByteReader}

private[finagle] object MuxFailure {
  private val ContextId = Buf.Utf8("MuxFailure")

  /**
   * Indicates that it is safe to re-issue the work.
   * Translates to [[com.twitter.finagle.FailureFlags.Retryable]]
   */
  val Retryable: Long = 1L << 0

  /**
   * Indicates that the request was rejected without being attempted.
   * Translates to [[com.twitter.finagle.FailureFlags.Rejected]]
   */
  val Rejected: Long = 1L << 1

  /**
   * Indicates that this work should not be retried at any level
   */
  val NonRetryable: Long = 1L << 2

  /**
   * A MuxFailure that contains no additional information
   */
  val Empty: MuxFailure = MuxFailure(0L)

  private val Extractor: PartialFunction[(Buf, Buf), MuxFailure] = {
    // Ignore anything after the first 8 bytes for future use
    case (k, vBuf) if k.equals(ContextId) && vBuf.length >= 8 =>
      val br = ByteReader(vBuf)
      try MuxFailure(br.readLongBE())
      finally br.close()
  }

  /**
   * Read a [[MuxFailure]] off of a list of response context key value pairs.
   */
  def fromContexts(contexts: Seq[(Buf, Buf)]): Option[MuxFailure] = {
    contexts.collectFirst(Extractor)
  }

  /**
   * Mask that covers representable mux failures.
   *
   * Note that this uses the FailureFlags representations of the failures, so
   * it can be used with FailureFlags for checking whether any of them match
   * MuxFailure failures, but cannot be used for checking MuxFailure failures.
   */
  private[this] val Mask: Long =
    FailureFlags.Retryable | FailureFlags.Rejected | FailureFlags.NonRetryable

  /**
   * Generate a [[MuxFailure]] from a Throwable where possible. If it is a
   * [[com.twitter.finagle.Failure]], then flags which have [[MuxFailure]]
   * analogs will be translated.
   */
  val FromThrow: PartialFunction[Throwable, MuxFailure] = {
    case f: FailureFlags[_] if (f.flags & Mask) != 0 =>
      var flags = 0L
      if (f.isFlagged(FailureFlags.Retryable)) flags |= Retryable
      if (f.isFlagged(FailureFlags.Rejected)) flags |= Rejected
      if (f.isFlagged(FailureFlags.NonRetryable)) flags |= NonRetryable
      MuxFailure(flags)
  }
}

/**
 * A mux-specific failure message which allows passing of additional metadata
 * along with failures across service boundaries. This allows for passing
 * [[com.twitter.finagle.Failure]]s via mux. In the future, this could be
 * expanded to support additional failure information.
 * Information is passed via the response contexts Rdispatch messages.
 *
 * As in [[com.twitter.finagle.Failure]], unrecognized flags are permitted for
 * future compatibility.
 */
private[mux] case class MuxFailure(flags: Long) {
  import MuxFailure._

  if (isFlagged(Retryable) && isFlagged(NonRetryable)) {
    assert(false, "Cannot be both Retryable and NonRetryable")
  }

  def isFlagged(which: Long): Boolean = (flags & which) == which

  /**
   * Generate [[com.twitter.finagle.Failure]] flags. Only flags which have
   * [[com.twitter.finagle.Failure]] flag analogs will be translated.
   *
   * @see [[com.twitter.finagle.FailureFlags.NonRetryable]],
   *      [[com.twitter.finagle.FailureFlags.Retryable]],
   *      [[com.twitter.finagle.FailureFlags.Rejected]]
   */
  def finagleFlags: Long = {
    var finagleFlags = 0L

    if (isFlagged(NonRetryable)) finagleFlags |= FailureFlags.NonRetryable
    if (isFlagged(Retryable)) finagleFlags |= FailureFlags.Retryable
    if (isFlagged(Rejected)) finagleFlags |= FailureFlags.Rejected

    finagleFlags
  }

  /**
   * Write a MuxFailure out as a Mux context pair if the flags convey any info,
   * otherwise, Nil. This returns a 0 or 1 element sequence rather than an Option
   * so it can be inserted directly in to an Rdispatch message's context without
   * conversion.
   */
  def contexts: Seq[(Buf, Buf)] = {
    if (this == Empty) Nil
    else Seq((ContextId, BufByteWriter.fixed(8).writeLongBE(flags).owned()))
  }
}
