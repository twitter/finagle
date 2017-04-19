package com.twitter.finagle

import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.Future
import scala.annotation.tailrec
import scala.util.control.NoStackTrace

/**
 * Base exception for all Finagle originated failures. These are
 * Exceptions, but with additional `sources` and `flags`. Sources
 * describe the origins of the failure to aid in debugging and flags
 * mark attributes of the Failure (e.g. Restartable).
 */
final class Failure private[finagle](
    private[finagle] val why: String,
    val cause: Option[Throwable] = None,
    val flags: Long = FailureFlags.Empty,
    protected val sources: Map[Failure.Source.Value, Object] = Map.empty,
    val logLevel: Level = Level.WARNING)
  extends Exception(why, cause.orNull)
  with NoStackTrace
  with HasLogLevel
  with FailureFlags[Failure]
{
  import Failure._

  require(!isFlagged(Wrapped) || cause.isDefined)

  /**
   * Returns a source for a given key, if it exists.
   */
  def getSource(key: Failure.Source.Value): Option[Object] = sources.get(key)

  /**
   * Creates a new Failure with the given key value pair prepended to sources.
   */
  def withSource(key: Failure.Source.Value, value: Object): Failure =
    copy(sources = sources + (key -> value))

  /**
   * A new failure with the current [[Failure]] as cause.
   */
  def chained: Failure = copy(cause=Some(this))

  /**
   * Creates a new Failure with the given logging Level.
   *
   * Note: it is not guaranteed that all `Failure`s are logged
   * within finagle and this only applies to ones that are.
   */
  def withLogLevel(level: Level): Failure =
    copy(logLevel = level)

  /**
   * A `Throwable` appropriate for user presentation (e.g., for stats,
   * or to return from a user's [[Service]].)
   *
   * Show may return `this`.
   */
  def show: Throwable = Failure.show(this)

  override def toString: String =
    "Failure(%s, flags=0x%02x) with %s".format(why, flags,
      if (sources.isEmpty) "NoSources" else sources.mkString(" with "))

  override def equals(a: Any): Boolean = {
    a match {
      case that: Failure =>
        this.why.equals(that.why) &&
        this.cause.equals(that.cause) &&
        this.flags.equals(that.flags) &&
        this.sources.equals(that.sources)
      case _ => false
    }
  }

  override def hashCode: Int =
    why.hashCode ^
    cause.hashCode ^
    flags.hashCode ^
    sources.hashCode

  private[this] def copy(
    why: String = why,
    cause: Option[Throwable] = cause,
    flags: Long = flags,
    sources: Map[Failure.Source.Value, Object] = sources,
    logLevel: Level = logLevel
  ): Failure = new Failure(why, cause, flags, sources, logLevel)

  protected def copyWithFlags(newFlags: Long): Failure = copy(flags = newFlags)
}

object Failure {
  object Source extends Enumeration {
    val
      /**
       * Represents the name of the service.
       * See [[com.twitter.finagle.filter.ExceptionSourceFilter]]
       */
      Service,

      /**
       * Represents a [[Stack.Role Stack module's role]].
       */
      Role,

      /**
       * Represents the remote info of the upstream caller and/or
       * downstream backend.
       * See [[com.twitter.finagle.service.ExceptionRemoteInfoFactory]]
       */
      RemoteInfo,

      /**
       * Represents the name of the method.
       * See [[com.twitter.finagle.client.MethodBuilder]].
       */
      Method
    = Value
  }

  /**
   * Flag restartable indicates that the action that caused the failure
   * is ''restartable'' -- that is, it is safe to simply re-issue the action.
   */
  val Restartable: Long = FailureFlags.Retryable

  /**
   * Flag interrupted indicates that the error was caused due to an
   * interruption. (e.g., by invoking [[Future.raise]].)
   */
  val Interrupted: Long = FailureFlags.Interrupted

  /**
   * Flag wrapped indicates that this failure was wrapped, and should
   * not be presented to the user (directly, or via stats). Rather, it must
   * first be unwrapped: the inner cause is the presentable failure.
   */
  val Wrapped: Long = FailureFlags.Wrapped

  /**
   * Flag rejected indicates that the work was rejected and therefore cannot be
   * completed. This may indicate an overload condition.
   */
  val Rejected: Long = FailureFlags.Rejected

  /**
   * Flag nonretryable indicates that the action that caused this failure should
   * not be re-issued. This failure should be propagated back along the call
   * chain as far as possible.
   */
  val NonRetryable: Long = FailureFlags.NonRetryable

  /**
   * Representation of a nack response that is retryable
   */
  val RetryableNackFailure: Failure = Failure.rejected("The request was Nacked by the server")

  /**
   * Representation of a future nack response that is retryable
   */
  val FutureRetryableNackFailure: Future[Nothing] = Future.exception(RetryableNackFailure)

  /**
   * Representation of a nack response that is non-retryable
   */
  val NonRetryableNackFailure: Failure =
    Failure("The request was Nacked by the server and should not be retried", Failure.Rejected|Failure.NonRetryable)

  /**
   * Flag naming indicates a naming failure. This is Finagle-internal.
   */
  private[finagle] val Naming: Long = FailureFlags.Naming

  /**
   * The mask of flags which are safe to show to users. As an example, showing
   * [[FailureFlags.Retryable]] could be dangerous when such failures are passed
   * back to Finagle servers. While an individual client's request is
   * restartable, the same is not automatically true of the server request on
   * whose behalf the client is working - it may have performed some side
   * effect before issuing the client call.
   */
  private val ShowMask: Long = FailureFlags.ShowMask

  /**
   * Create a new failure with the given cause and flags.
   */
  def apply(cause: Throwable, flags: Long, logLevel: Level = Level.WARNING): Failure =
    if (cause == null)
      new Failure("unknown", None, flags, logLevel = logLevel)
    else if (cause.getMessage == null)
      new Failure(cause.getClass.getName, Some(cause), flags, logLevel = logLevel)
    else
      new Failure(cause.getMessage, Some(cause), flags, logLevel = logLevel)

  private[this] def computeLogLevel(t: Throwable): Level = t match {
    case HasLogLevel(level) => level
    case _ => Level.WARNING
  }

  /**
   * Create a new failure with the given cause; no flags.
   */
  def apply(cause: Throwable): Failure =
    apply(cause, 0L, computeLogLevel(cause))

  /**
   * Create a new failure with the given message, cause, and flags.
   */
  def apply(why: String, cause: Throwable, flags: Long): Failure =
    new Failure(why, Option(cause), flags, logLevel = computeLogLevel(cause))

  /**
   * Create a new failure with the given message and cause; no flags.
   */
  def apply(why: String, cause: Throwable): Failure =
    new Failure(why, Option(cause), 0L, logLevel = computeLogLevel(cause))

  /**
   * Create a new failure with the given message and flags.
   */
  def apply(why: String, flags: Long): Failure =
    new Failure(why, None, flags)

  /**
   * Create a new failure with the given message; no flags.
   */
  def apply(why: String): Failure =
    new Failure(why, None, 0L)

  /**
   * Extractor for [[Failure]]; returns its cause.
   */
  def unapply(exc: Failure): Option[Option[Throwable]] = Some(exc.cause)

  /**
   * Expose flags as strings, used for stats reporting
   */
  def flagsOf(exc: Throwable): Set[String] =
    exc match {
      case f: Failure => FailureFlags.flagsOf(f.flags)
      case _ => Set.empty
    }

  /**
   * Adapt an exception. If the passed-in exception is already a failure,
   * this returns a chained failure with the assigned flags. If it is not,
   * it returns a new failure with the given flags.
   */
  def adapt(exc: Throwable, flags: Long): Failure = exc match {
    case f: Failure => f.chained.flagged(flags)
    case _ => Failure(exc, flags, computeLogLevel(exc))
  }

  /**
   * Create a new wrapped Failure with the given flags. If the passed-in exception
   * is a failure, it is simply extended, otherwise a new Failure is created.
   */
  def wrap(exc: Throwable, flags: Long): Failure = {
    require(exc != null)
    exc match {
      case f: Failure => f.flagged(flags|Failure.Wrapped)
      case _ => Failure(exc, flags|Failure.Wrapped, computeLogLevel(exc))
    }
  }

  /**
   * Create a new wrapped Failure with no flags. If the passed-in exception
   * is a failure, it is simply extended, otherwise a new Failure is created.
   */
  def wrap(exc: Throwable): Failure =
    wrap(exc, 0L)

  /**
   * Create a new [[Restartable]] and [[Rejected]] failure with the given message.
   */
  def rejected(why: String): Failure =
    new Failure(why, None, FailureFlags.Retryable | FailureFlags.Rejected, logLevel = Level.DEBUG)

  /**
   * Create a new [[Restartable]] and [[Rejected]] failure with the given cause.
   */
  def rejected(cause: Throwable): Failure =
    Failure(cause, FailureFlags.Retryable | FailureFlags.Rejected, logLevel = Level.DEBUG)

  /**
   * Create a new [[Restartable]] and [[Rejected]] failure with the given
   * message and cause.
   */
  def rejected(why: String, cause: Throwable): Failure =
    new Failure(why, Option(cause), FailureFlags.Retryable | FailureFlags.Rejected, logLevel = Level.DEBUG)

  /**
   * A default [[Restartable]] failure.
   */
  val rejected: Failure = rejected("The request was rejected")

  @tailrec
  private def show(f: Failure): Throwable = {
    if (!f.isFlagged(FailureFlags.Wrapped)) f.masked(ShowMask)
    else f.cause match {
      case Some(inner: Failure) => show(inner)
      case Some(inner: Throwable) => inner
      case None =>
        throw new IllegalArgumentException("Wrapped failure without a cause")
    }
  }

  /**
   * Process failures for external presentation. Specifically, this converts
   * failures to their "showable" form, unwrapping inner failures/throwables and
   * masking off certain flags. See [[FailureFlags.ShowMask]].
   */
  private[finagle] class ProcessFailures[Req, Rep] extends SimpleFilter[Req, Rep] {
    private[this] val Process: PartialFunction[Throwable, Future[Rep]] = {
      case f: Failure => Future.exception(f.show)
      case f: FailureFlags[_] => {
        Future.exception(f.masked(FailureFlags.ShowMask))
      }
    }

    def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
      service(req).rescue(Process)
  }

  val role: Stack.Role = Stack.Role("ProcessFailure")

  /**
   * A module to strip out dangerous flags.
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role: Stack.Role = Failure.role
      val description: String = "process failures"

      private[this] val filter = new ProcessFailures[Req, Rep]

      def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        filter.andThen(next)
    }
}
