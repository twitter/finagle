package com.twitter.finagle

/**
 * Base exception for all Finagle originated failures. These are
 * Exceptions, but with additional `sources` and `flags`.
 * Sources describe the origins of the failure to aid in debugging
 * and flags mark attributes of the Failure (e.g. Retryable).
 * Failures are constructed and extracted using the Failure
 * companion object.
 *
 * {{{
 * val failure = Failure.InterruptedBy(cause).withRetryable(true)
 * val shouldRetry = failure match {
 *  case Failure.Retryable(_) => true
 *  case _ => false
 * }
 * }}}
 */
final class Failure private[finagle](
  private[finagle] val why: String,
  val cause: Throwable = null,
  val flags: Long = Failure.Flag.None,
  sources: Map[String, Object] = Map(),
  val stacktrace: Array[StackTraceElement] = Failure.NoStacktrace
) extends Exception(why, cause) with NoStacktrace {
  import Failure._

  /**
   * Returns a source for a given key, if it exists.
   */
  def getSource(key: String): Option[Object] = sources.get(key)

  /**
   * Creates a new Failure with the given key value pair prepended to sources.
   */
  def withSource(key: String, value: Object): Failure =
    copy(sources = sources + (key -> value))

  /**
   * Creates a new Failure with the current threads stacktrace.
   */
  def withStackTrace(): Failure =
    copy(stacktrace = Thread.currentThread.getStackTrace())

  /**
   * Creates a new Failure that toggles retryable based on the given boolean.
   */
  def withRetryable(on: Boolean): Failure =
    if (on == isSet(flags, Flag.Retryable)) this else {
      copy(flags = toggle(on, flags, Flag.Retryable))
    }

  override def toString: String =
    "Failure(%s, flags=0x%02x)\n\twith %s".format(why, flags,
      if (sources.isEmpty) "NoSources" else sources.mkString("\n\twith "))

  override def getStackTrace(): Array[StackTraceElement] = stacktrace
  override def printStackTrace(p: java.io.PrintWriter) {
    p.println(this)
    for (te <- stacktrace)
      p.println("\tat %s".format(te))
  }

  override def equals(a: Any) = {
    a match {
      case Failure(cause, flags) => this.cause == cause && this.flags == flags
      case _ => false
    }
  }

  override def hashCode = cause.hashCode ^ flags.hashCode

  def copy(
    why: String = why,
    cause: Throwable = cause,
    flags: Long = flags,
    sources: Map[String, Object] = sources,
    stacktrace: Array[StackTraceElement] = stacktrace
  ): Failure = new Failure(why, cause, flags, sources, stacktrace)
}

/**
 * Defines convenient methods for contructing and extracting failures.
 */
object Failure {
  private val NoStacktrace =
    Array(new StackTraceElement("com.twitter.finagle", "NoStacktrace", null, -1))

  object Sources {
    val ServiceName = "service name"
  }

  def unapply(exc: Failure): Option[(Throwable, Long)] = Some((exc.cause, exc.flags))

  /**
   * Failure attributes are distinguished by flags.
   * Flags should be a power of 2.
   */
  private object Flag {
    val None          = 0L
    val Retryable     = 1L << 0
    val Interrupted   = 1L << 1
    // Bits 32 to 63 are reserved for
    // flags private to finagle.
    val Requeueable    = 1L << 32
  }

  private[this] def validate(flag: Long) =
    require((flag & flag-1) == 0, "flag should be a power of 2")

  private def isSet(flags: Long, flag: Long) = {
    validate(flag)
    (flags & flag) != 0
  }

  private def toggle(on: Boolean, flags: Long, flag: Long) = {
    validate(flag)
    if (on) flags | flag else flags & (~flag)
  }

  /**
   * Defines methods to create a Failure type with the given `flag` bit.
   */
  trait Injections {
    val flag: Long
    def apply(why: String, cause: Throwable = null): Failure = new Failure(why, cause, flag)
    def apply(cause: Throwable): Failure =
      if (cause == null) apply("unknown cause")
      else if (cause.getMessage == null) apply(cause.getClass.getName, cause)
      else apply(cause.getMessage, cause)
  }

  /**
   * Defines methods to extract a Failure based on the `flag` bit. Note that if
   * an underlying cause is set, the underlying exception is extracted.
   */
  trait Extractions {
    val flag: Long
    def unapply(exc: Throwable): Option[Throwable] = exc match {
      case f@Failure(null, fs) if isSet(fs, flag) => Some(f)
      case Failure(cause, fs) if isSet(fs, flag) => Some(cause)
      case _ => None
    }
  }

  /**
   * A Failure with a throwable cause.
   */
  object Cause extends Injections {
    val flag = Flag.None
    def unapply(exc: Throwable): Option[Throwable] = exc match {
      case Failure(cause, _) if cause != null => Some(cause)
      case _ => None
    }
  }

  /**
   * A Failure indicating that the corresponding request future has been interrupted.
   * Note, this does does not guarantee that the request was not dispatched.
   */
  object InterruptedBy extends Injections with Extractions {
    val flag = Flag.Interrupted
  }

  /**
   * A retryable failure indicates that thw corresponding dispatch has failed and
   * is eligible for a retry. These types of failures should count against a retry
   * budget.
   */
  object Retryable extends Injections with Extractions {
    val flag = Flag.Retryable
  }

  /**
   * A requeueable failure indicates that a corresponding dispatch has failed
   * and finagle can safely attempt to redispatch. These failures should be
   * handled by finagle and should not count against any user defined retry
   * budget.
   */
  private[finagle] object Requeueable extends Injections with Extractions {
    val flag = Flag.Requeueable
  }
}
