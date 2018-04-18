package com.twitter.finagle

import com.twitter.util.{Future, Throw, Try}

/**
 * `FailureFlags` may be applied to any Failure/Exception encountered during the
 * handling of a request.
 */
object FailureFlags {

  val Empty: Long = 0L

  /**
   * Retryable indicates that the action that caused the failure is known
   * to be safe to retry. The [[RequeueFilter]] will automatically retry any
   * such failures. Note that this is independent of any user-configured retry
   * logic. This is Finagle-internal.
   */
  private[finagle] val Retryable: Long = 1L << 0

  /**
   * Interrupted indicates that the error was caused due to an
   * interruption. (e.g., by invoking [[com.twitter.util.Future.raise]].)
   */
  val Interrupted: Long = 1L << 1

  /**
   * Wrapped indicates that this failure was wrapped, and should
   * not be presented to the user (directly, or via stats). Rather, it must
   * first be unwrapped: the inner cause is the presentable failure.
   */
  private[finagle] val Wrapped: Long = 1L << 2

  /**
   * Rejected indicates that the work was rejected and therefore cannot be
   * completed. This may indicate an overload condition.
   */
  val Rejected: Long = 1L << 3

  /**
   * NonRetryable indicates that the action that caused this failure should
   * not be re-issued. This failure should be propagated back along the call
   * chain as far as possible.
   */
  val NonRetryable: Long = 1L << 4

  /**
   * Ignorable indicates that this failure can be ignored and should not be surfaced via stats.
   */
  private[twitter] val Ignorable: Long = 1L << 5

  /**
   * Naming indicates a naming failure. This is Finagle-internal.
   */
  private[finagle] val Naming: Long = 1L << 32

  /**
   * The mask of flags which are safe to show to users. As an example, showing
   * [[Retryable]] could be dangerous when such failures are passed
   * back to Finagle servers. While an individual client's request is
   * retryable, the same is not automatically true of the server request on
   * whose behalf the client is working - it may have performed some side
   * effect before issuing the client call.
   */
  private[finagle] val ShowMask: Long = Interrupted | Rejected | NonRetryable

  /**
   * Expose flags as strings. Used for stats reporting. Here, Retryable is named
   * "restartable" for now to maintain compatibility with existing stats.
   */
  def flagsOf(flags: Long): Set[String] = {
    var names: Set[String] = Set.empty
    if ((flags & Interrupted) > 0) names += "interrupted"
    if ((flags & Retryable) > 0) names += "restartable" // See doc
    if ((flags & Wrapped) > 0) names += "wrapped"
    if ((flags & Rejected) > 0) names += "rejected"
    if ((flags & Naming) > 0) names += "naming"
    if ((flags & NonRetryable) > 0) names += "nonretryable"
    names
  }

  /**
   * Expose flags of a given throwable as strings. Here, Retryable is named
   * "restartable" for now to maintain compatibility with existing stats.
   */
  def flagsOf(e: Throwable): Set[String] = e match {
    case f: FailureFlags[_] => flagsOf(f.flags)
    case _ => Set.empty
  }

  /**
   * A function for transforming unsuccessful responses into ones that are
   * flagged as NonRetryable
   */
  private[finagle] def asNonRetryable[Rep](t: Try[Rep]): Future[Rep] = {
    t match {
      case Throw(f: FailureFlags[_]) => Future.exception(f.asNonRetryable)
      case Throw(exn) => Future.exception(Failure(exn, FailureFlags.NonRetryable))
      case _ => Future.const(t)
    }
  }

  /**
   * A way for non-finagle folks to test if a throwable is flagged
   */
  def isFlagged(flags: Long)(t: Throwable): Boolean = t match {
    case f: FailureFlags[_] => f.isFlagged(flags)
    case _ => false
  }
}

/**
 * A trait for exceptions that are flagged with the additional attributes
 * defined above.
 */
private[finagle] trait FailureFlags[T <: FailureFlags[T]] extends Throwable { this: T =>
  import FailureFlags._

  require(!isFlagged(Retryable | NonRetryable), "Cannot be flagged both Retryable and NonRetryable")

  private[finagle] def flags: Long

  /**
   * Test if this is flagged with a particular set of flags
   */
  def isFlagged(which: Long): Boolean = (flags & which) == which

  /**
   * This as a non-retryable failure. This does not mutate.
   */
  def asNonRetryable: T = {
    unflagged(Retryable).flagged(NonRetryable)
  }

  /**
   * This as a rejected failure. This does not mutate.
   */
  def asRejected: T = {
    flagged(Rejected)
  }

  /**
   * A copy of this object with the given flags replacing the current flags. The
   * caller of this method should check to see if a copy is necessary before
   * calling.
   *
   * As this is an internal API, the other `Throwable` fields such as the cause
   * and stack trace should be handled by callers.
   */
  protected def copyWithFlags(flags: Long): T

  /**
   * This with the current flags replaced by newFlags. This does not mutate.
   */
  private[finagle] def withFlags(newFlags: Long): T =
    if (newFlags == flags) {
      this
    } else {
      val copied = copyWithFlags(newFlags)
      copied.setStackTrace(getStackTrace)
      if (getCause != null && copied.getCause == null) {
        copied.initCause(getCause)
      }
      getSuppressed.toSeq.foreach { t =>
        copied.addSuppressed(t)
      }
      copied
    }

  /**
   * This with the given flags added. This does not mutate.
   */
  private[finagle] def flagged(addFlags: Long): T = withFlags(flags | addFlags)

  /**
   * This with the given flags removed. This does not mutate.
   */
  private[finagle] def unflagged(delFlags: Long): T = withFlags(flags & ~delFlags)

  /**
   * This with the mask applied. This does not mutate.
   */
  private[finagle] def masked(mask: Long): T = withFlags(flags & mask)
}
