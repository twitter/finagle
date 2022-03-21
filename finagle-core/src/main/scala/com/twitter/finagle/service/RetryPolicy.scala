package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Backoff
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.Failure
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.TimeoutException
import com.twitter.finagle.WriteException
import com.twitter.util.Duration
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import com.twitter.util.{TimeoutException => UtilTimeoutException}

/**
 * A function defining retry behavior for a given value type `A`.
 *
 * The [[Function1]] returns [[None]] if no more retries should be made
 * and [[Some]] if another retry should happen. The returned `Some` has
 * a [[Duration]] field for how long to wait for the next retry as well
 * as the next `RetryPolicy` to use.
 *
 * Finagle will automatically handle retryable Throws. Requests that
 * have not been written to or processed by a remote service are safe to
 * retry.
 *
 * @see [[SimpleRetryPolicy]] for a Java friendly API.
 */
abstract class RetryPolicy[-A] extends (A => Option[(Duration, RetryPolicy[A])]) {

  /**
   * Creates a new `RetryPolicy` based on the current `RetryPolicy` in which values of `A`
   * are first checked against a predicate function, and only if the predicate returns true
   * will the value be passed on to the current `RetryPolicy`.
   *
   * The predicate function need not be a pure function, but can change its behavior over
   * time.  For example, the predicate function's decision can be based upon backpressure
   * signals supplied by things like failure rates or latency, which allows `RetryPolicy`s
   * to dynamically reduce the number of retries in response to backpressure.
   *
   * The predicate function is only called on the first failure in a chain. Any additional
   * chained RetryPolicies returned by the current policy will then see additional failures
   * unfiltered.  Contrast this will `filterEach`, which applies the filter to each `RetryPolicy`
   * in the chain.
   */
  def filter[B <: A](pred: B => Boolean): RetryPolicy[B] =
    RetryPolicy.named("RetryPolicy.filter") { e => if (!pred(e)) None else this(e) }

  /**
   * Similar to `filter`, but the predicate is applied to each `RetryPolicy` in the chain
   * returned by the current RetryPolicy.  For example, if the current `RetryPolicy` returns
   * `Some((D, P'))` for value `E` (of type `A`), and the given predicate returns true for `E`,
   * then the value returned from the filtering `RetryPolicy` will be `Some((D, P''))` where
   * `P''` is equal to `P'.filterEach(pred)`.
   *
   * One example where this is useful is to dynamically and fractionally allow retries based
   * upon backpressure signals.  If, for example, the predicate function returned true or false
   * based upon a probability distribution computed from a backpressure signal, it could return
   * true 50% of the time, giving you a 50% chance of performing a single retry, a 25% chance of
   * performing 2 retries, 12.5% chance of performing 3 retries, etc.  This might be more
   * desirable than just using `filter` where you end up with a 50% chance of no retries and
   * 50% chance of the full number of retries.
   */
  def filterEach[B <: A](pred: B => Boolean): RetryPolicy[B] =
    RetryPolicy.named("RetryPolicy.filterEach") { e =>
      if (!pred(e))
        None
      else {
        this(e).map {
          case (backoff, p2) => (backoff, p2.filterEach(pred))
        }
      }
    }

  /**
   * Applies a dynamically chosen retry limit to an existing `RetryPolicy` that may allow for
   * more retries.  When the returned `RetryPolicy` is first invoked, it will call the `maxRetries`
   * by-name parameter to get the current maximum retries allowed.  Regardless of the number
   * of retries that the underlying policy would allow, it is capped to be no greater than the
   * number returned by `maxRetries` on the first failure in the chain.
   *
   * Using a dynamically chosen retry limit allows for the retry count to be tuned at runtime
   * based upon backpressure signals such as failure rate or request latency.
   */
  def limit(maxRetries: => Int): RetryPolicy[A] =
    RetryPolicy.named[A]("RetryPolicy.limit") { e =>
      val triesRemaining = maxRetries
      if (triesRemaining <= 0)
        None
      else {
        this(e).map {
          case (backoff, p2) => (backoff, p2.limit(triesRemaining - 1))
        }
      }
    }

  override def toString: String = getClass.getName
}

/**
 * A retry policy abstract class. This is convenient to use for Java programmers. Simply implement
 * the two abstract methods `shouldRetry` and `backoffAt` and you're good to go!
 */
abstract class SimpleRetryPolicy[A](i: Int)
    extends RetryPolicy[A]
    with (A => Option[(Duration, RetryPolicy[A])]) {
  def this() = this(0)

  final def apply(e: A): Option[(Duration, RetryPolicy[A])] = {
    if (shouldRetry(e)) {
      backoffAt(i) match {
        case Duration.Top =>
          None
        case howlong =>
          Some(
            (
              howlong,
              new SimpleRetryPolicy[A](i + 1) {
                def shouldRetry(a: A) = SimpleRetryPolicy.this.shouldRetry(a)
                def backoffAt(retry: Int) = SimpleRetryPolicy.this.backoffAt(retry)
                override def toString: String = SimpleRetryPolicy.this.toString
              }))
      }
    } else {
      None
    }
  }

  override def andThen[B](that: Option[(Duration, RetryPolicy[A])] => B): A => B =
    that.compose(this)

  override def compose[B](that: B => A): B => Option[(Duration, RetryPolicy[A])] =
    that.andThen(this)

  /**
   * Given a value, decide whether it is retryable. Typically the value is an exception.
   */
  def shouldRetry(a: A): Boolean

  /**
   * Given a number of retries, return how long to wait till the next retry. Note that this is
   * zero-indexed. To implement a finite number of retries, implement a method like:
   *     `if (i > 3) return never`
   */
  def backoffAt(retry: Int): Duration

  /**
   * A convenience method to access Duration.Top from Java. This is a sentinel value that
   * signals no-further-retries.
   */
  final val never = Duration.Top
}

object RetryPolicy {

  // We provide a proxy around partial functions so we can give them a better .toString
  private class NamedPf[A, B](name: String, f: PartialFunction[A, B])
      extends PartialFunction[A, B] {
    def isDefinedAt(x: A): Boolean = f.isDefinedAt(x)
    def apply(v1: A): B = f(v1)

    override def applyOrElse[A1 <: A, B1 >: B](
      x: A1,
      default: A1 => B1
    ): B1 = f.applyOrElse(x, default)

    override def orElse[A1 <: A, B1 >: B](that: PartialFunction[A1, B1]): PartialFunction[A1, B1] =
      new NamedPf(s"$name.orElse($that)", f.orElse(that))

    override def lift: A => Option[B] = f.lift

    override def toString(): String = name
  }

  /**
   * Create a `PartialFunction` with a defined `.toString` method.
   *
   * @param name the value to use for `toString`
   * @param f the `PartialFunction` to apply the name to.
   */
  def namedPF[A](
    name: String
  )(
    f: PartialFunction[A, Boolean]
  ): PartialFunction[A, Boolean] =
    new NamedPf(name, f)

  /**
   * An extractor for exceptions which are known to be safe to retry.
   *
   * @see [[RequeueFilter.Requeueable]]
   */
  object RetryableWriteException {
    def unapply(thr: Throwable): Option[Throwable] = thr match {
      // We don't retry interruptions by default since they indicate that the
      // request was discarded.
      case f: FailureFlags[_] if f.isFlagged(FailureFlags.Interrupted) => None
      case f: FailureFlags[_] if f.isFlagged(FailureFlags.NonRetryable) => None
      case f: Failure if f.isFlagged(FailureFlags.Retryable) => Some(f.show)
      case f: FailureFlags[_] if f.isFlagged(FailureFlags.Retryable) => Some(f)
      case WriteException(exc) => Some(exc)
      case _ => None
    }
  }

  /**
   * Failures that are generally retryable because the request failed
   * before it finished being written to the remote service.
   * See [[com.twitter.finagle.WriteException]].
   */
  val WriteExceptionsOnly: PartialFunction[Try[Nothing], Boolean] =
    namedPF("WriteExceptionsOnly") {
      case Throw(RetryableWriteException(_)) => true
    }

  /**
   * Use [[ResponseClassifier.RetryOnTimeout]] composed with
   * [[ResponseClassifier.RetryOnWriteExceptions]] for the [[ResponseClassifier]] equivalent.
   */
  val TimeoutAndWriteExceptionsOnly: PartialFunction[Try[Nothing], Boolean] =
    namedPF("TimeoutAndWriteExceptionsOnly")(WriteExceptionsOnly.orElse {
      case Throw(Failure(Some(_: TimeoutException))) => true
      case Throw(Failure(Some(_: UtilTimeoutException))) => true
      case Throw(_: TimeoutException) => true
      case Throw(_: UtilTimeoutException) => true
    })

  /**
   * Use [[ResponseClassifier.RetryOnChannelClosed]] for the [[ResponseClassifier]] equivalent.
   */
  val ChannelClosedExceptionsOnly: PartialFunction[Try[Nothing], Boolean] =
    namedPF("ChannelClosedExceptionsOnly") {
      case Throw(_: ChannelClosedException) => true
    }

  /**
   * A [[RetryPolicy]] that never retries.
   */
  val none: RetryPolicy[Any] = new RetryPolicy[Any] {
    def apply(t: Any): Option[(Duration, RetryPolicy[Any])] = None
    override def toString: String = "RetryPolicy.none"
  }

  /**
   * A [[RetryPolicy]] that never retries.
   *
   * @see [[none]] for a more generally applicable version.
   */
  val Never: RetryPolicy[Try[Nothing]] = none

  /**
   * Converts a `RetryPolicy[Try[Nothing]]` to a `RetryPolicy[(Req, Try[Rep])]`
   * that acts only on exceptions.
   */
  private[finagle] def convertExceptionPolicy[Req, Rep](
    policy: RetryPolicy[Try[Nothing]]
  ): RetryPolicy[(Req, Try[Rep])] =
    new RetryPolicy[(Req, Try[Rep])] {
      def apply(input: (Req, Try[Rep])): Option[(Duration, RetryPolicy[(Req, Try[Rep])])] =
        input match {
          case (_, t @ Throw(_)) =>
            policy(t.cast[Nothing]) match {
              case Some((howlong, nextPolicy)) =>
                Some((howlong, convertExceptionPolicy(nextPolicy)))
              case None => None
            }
          case (_, Return(_)) => None
        }

      override def toString: String = s"RetryPolicy.convertExceptionPolicy($policy)"
    }

  /**
   * Lifts a function of type `A => Option[(Duration, RetryPolicy[A])]` in the  `RetryPolicy` type.
   *
   * @param f The function used to classify values.
   *
   * @note the [[RetryPolicy.named]] function should be preferred to make inspection of the
   *       [[RetryPolicy]] easier.
   */
  def apply[A](f: A => Option[(Duration, RetryPolicy[A])]): RetryPolicy[A] =
    named(s"RetryPolicy.apply($f)")(f)

  /**
   * Lifts a function of type `A => Option[(Duration, RetryPolicy[A])]` in the  `RetryPolicy` type.
   *
   * @param name The name to use in the `.toString` method which can aid in inspection.
   * @param f The function used to classify values.
   */
  def named[A](name: String)(f: A => Option[(Duration, RetryPolicy[A])]): RetryPolicy[A] =
    new RetryPolicy[A] {
      def apply(e: A): Option[(Duration, RetryPolicy[A])] = f(e)
      override def toString: String = name
    }

  /**
   * Try up to a specific number of times, based on the supplied `PartialFunction[A, Boolean]`.
   * A value of type `A` is considered retryable if and only if the PartialFunction
   * is defined at and returns true for that value.
   *
   * The returned policy has jittered backoffs between retries.
   *
   * @param numTries the maximum number of attempts (including retries) that can be made.
   *   A value of `1` means one attempt and no retries on failure.
   *   A value of `2` means one attempt and then a single retry if the failure meets the
   *   criteria of `shouldRetry`.
   * @param shouldRetry which `A`-typed values are considered retryable.
   */
  def tries[A](numTries: Int, shouldRetry: PartialFunction[A, Boolean]): RetryPolicy[A] = {
    val backoffs = Backoff.decorrelatedJittered(5.millis, 200.millis)
    backoff[A](backoffs.take(numTries - 1))(shouldRetry)
  }

  /**
   * Try up to a specific number of times of times on failures that are
   * [[com.twitter.finagle.service.RetryPolicy.WriteExceptionsOnly]].
   *
   * The returned policy has jittered backoffs between retries.
   *
   * @param numTries the maximum number of attempts (including retries) that can be made.
   *   A value of `1` means one attempt and no retries on failure.
   *   A value of `2` means one attempt and then a single retry if the failure meets the
   *   criteria of [[com.twitter.finagle.service.RetryPolicy.WriteExceptionsOnly]].
   */
  def tries(numTries: Int): RetryPolicy[Try[Nothing]] = tries(numTries, WriteExceptionsOnly)

  private[this] val AlwaysFalse = Function.const(false) _

  /**
   * Retry based on a series of backoffs defined by a `Stream[Duration]`. The
   * stream is consulted to determine the duration after which a request is to
   * be retried. A `PartialFunction` argument determines which request types
   * are retryable.
   *
   * @see [[backoffJava]] for a Java friendly API.
   */
  def backoff[A](
    backoffs: Backoff
  )(
    shouldRetry: PartialFunction[A, Boolean]
  ): RetryPolicy[A] = {
    RetryPolicy.named(s"backoff($backoffs($shouldRetry)") { e =>
      if (shouldRetry.applyOrElse(e, AlwaysFalse) && !backoffs.isExhausted) {
        Some((backoffs.duration, backoff(backoffs.next)(shouldRetry)))
      } else {
        None
      }
    }
  }

  /**
   * @deprecated Please use the [[backoff]] api which is already Java friendly.
   *
   * A version of [[backoff]] usable from Java.
   *
   * @param backoffs can be created via [[Backoff.toJava]].
   */
  def backoffJava[A](
    backoffs: Backoff,
    shouldRetry: PartialFunction[A, Boolean]
  ): RetryPolicy[A] = {
    backoff[A](backoffs)(shouldRetry)
  }

  /**
   * Combines multiple `RetryPolicy`s into a single combined `RetryPolicy`, with interleaved
   * backoffs.  For a given value of `A`, each policy in `policies` is tried in order.  If all
   * policies return `None`, then the combined `RetryPolicy` returns `None`.  If policy `P` returns
   * `Some((D, P'))`, then the combined `RetryPolicy` returns `Some((D, P''))`, where `P''` is a
   * new combined `RetryPolicy` with the same sub-policies, with the exception of `P` replaced by
   * `P'`.
   *
   * The ordering of policies matters: earlier policies get a chance to handle the failure
   * before later policies; a catch-all policy, if any, should be last.
   *
   * As an example, let's say you combine two `RetryPolicy`s, `R1` and `R2`, where `R1` handles
   * only exception `E1` with a backoff of `(10.milliseconds, 20.milliseconds, 30.milliseconds)`,
   * while `R2` handles only exception `E2` with a backoff of `(15.milliseconds, 25.milliseconds)`.
   *
   * If a sequence of exceptions, `(E2, E1, E1, E2)`, is fed in order to the combined retry policy,
   * the backoffs seen will be `(15.milliseconds, 10.milliseconds, 20.milliseconds,
   * 25.milliseconds)`.
   *
   * The maximum number of retries the combined policy could allow under the worst case scenario
   * of exceptions is equal to the sum of the individual maximum retries of each subpolicy.  To
   * put a cap on the combined maximum number of retries, you can call `limit` on the combined
   * policy with a smaller cap.
   */
  def combine[A](policies: RetryPolicy[A]*): RetryPolicy[A] =
    RetryPolicy.named[A](s"RetryPolicy.combine(${policies.mkString(", ")})") { e =>
      // stores the first matched backoff
      var backoffOpt: Option[Duration] = None

      val policies2 =
        policies.map { p =>
          if (backoffOpt.nonEmpty)
            p
          else {
            p(e) match {
              case None => p
              case Some((backoff, p2)) =>
                backoffOpt = Some(backoff)
                p2
            }
          }
        }

      backoffOpt match {
        case None => None
        case Some(backoff) => Some((backoff, combine(policies2: _*)))
      }
    }
}
