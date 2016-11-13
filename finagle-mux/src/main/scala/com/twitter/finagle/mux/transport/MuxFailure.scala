package com.twitter.finagle.mux.transport

import com.twitter.finagle.Failure
import com.twitter.finagle.util.{BufWriter, BufReader}
import com.twitter.io.Buf

private[mux] object MuxFailure {
  private val ContextId = Buf.Utf8("MuxFailure")

  /**
   * Indicates that it is safe to re-issue the work.
   * Translates to [[com.twitter.finagle.Failure.Restartable]]
   */
  val Restartable: Long = 1L << 0

  /**
   * Indicates that the request was rejected without being attempted.
   * Translates to [[com.twitter.finagle.Failure.Rejected]]
   */
  val Rejected: Long = 1L << 1

  /**
   * Indicates that this work should not be retried at any level
   */
  val NonRetryable: Long = 1L << 2


  /**
   * A MuxFailure that contains no additional information
   */
  val Empty = MuxFailure(0L)

  private val Extractor: PartialFunction[(Buf, Buf), MuxFailure] = {
    // Ignore anything after the first 8 bytes for future use
    case (k, vBuf) if k.equals(ContextId) && vBuf.length >= 8 =>
      MuxFailure(BufReader(vBuf).readLongBE())
  }

  /**
   * Read a [[MuxFailure]] off of a list of response context key value pairs.
   */
  def fromContexts(contexts: Seq[(Buf, Buf)]): Option[MuxFailure] = {
    contexts.collectFirst(Extractor)
  }

  /**
   * Generate a [[MuxFailure]] from a Throwable. If it is a
   * [[com.twitter.finagle.Failure]], then flags which have [[MuxFailure]]
   * analogs will be translated.
   */
  def fromThrow(exc: Throwable): MuxFailure = {
    exc match {
      case f: Failure =>
        var flags = 0L
        if (f.isFlagged(Failure.Restartable)) flags |= Restartable
        if (f.isFlagged(Failure.Rejected)) flags |= Rejected
        if (f.isFlagged(Failure.NonRetryable)) flags |= NonRetryable
        MuxFailure(flags)

      case _ =>
        Empty
    }
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

  if (isFlagged(Restartable) && isFlagged(NonRetryable)) {
    assert(false, "Cannot be both Restartable and NonRetryable")
  }

  def isFlagged(which: Long): Boolean = (flags & which) == which

  /**
   * Generate [[com.twitter.finagle.Failure]] flags. Only flags which have
   * [[com.twitter.finagle.Failure]] flag analogs will be translated.
   *
   * @see [[com.twitter.finagle.Failure.NonRetryable]],
   *      [[com.twitter.finagle.Failure.Restartable]],
   *      [[com.twitter.finagle.Failure.Rejected]]
   */
  def finagleFlags: Long = {
    var finagleFlags = 0L

    if (isFlagged(NonRetryable)) finagleFlags |= Failure.NonRetryable
    if (isFlagged(Restartable)) finagleFlags |= Failure.Restartable
    if (isFlagged(Rejected)) finagleFlags |= Failure.Rejected

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
    else Seq((ContextId, BufWriter.fixed(8).writeLongBE(flags).owned()))
  }
}
