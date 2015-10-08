package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.{ChannelClosedException, Failure, TimeoutException, WriteException}
import com.twitter.util.{
  TimeoutException => UtilTimeoutException, Duration, JavaSingleton, Return, Throw, Try}
import java.util.{concurrent => juc}
import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * A function defining retry behavior for a given value type `A`.
 *
 * The [[Function1]] returns [[None]] if no more retries should be made
 * and [[Some]] if another retry should happen. The returned `Some` has
 * a [[Duration]] field for how long to wait for the next retry as well
 * as the next `RetryPolicy` to use.
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
    RetryPolicy { e =>
      if (!pred(e)) None else this(e)
    }

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
    RetryPolicy { e =>
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
   * Using a dynamically choosen retry limit allows for the retry count to be tuned at runtime
   * based upon backpressure signals such as failure rate or request latency.
   */
  def limit(maxRetries: => Int): RetryPolicy[A] =
    RetryPolicy[A] { e =>
      val triesRemaining = maxRetries
      if (triesRemaining <= 0)
        None
      else {
        this(e).map {
          case (backoff, p2) => (backoff, p2.limit(triesRemaining - 1))
        }
      }
    }
}

/**
 * A retry policy abstract class. This is convenient to use for Java programmers. Simply implement
 * the two abstract methods `shouldRetry` and `backoffAt` and you're good to go!
 */
abstract class SimpleRetryPolicy[A](i: Int) extends RetryPolicy[A]
  with (A => Option[(Duration, RetryPolicy[A])])
{
  def this() = this(0)

  final def apply(e: A) = {
    if (shouldRetry(e)) {
      backoffAt(i) match {
        case Duration.Top =>
          None
        case howlong =>
          Some((howlong, new SimpleRetryPolicy[A](i + 1) {
            def shouldRetry(a: A) = SimpleRetryPolicy.this.shouldRetry(a)
            def backoffAt(retry: Int) = SimpleRetryPolicy.this.backoffAt(retry)
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

object RetryPolicy extends JavaSingleton {
  object RetryableWriteException {
    def unapply(thr: Throwable): Option[Throwable] = thr match {
      // We don't retry interruptions by default since they
      // indicate that the request was discarded.
      case f: Failure if f.isFlagged(Failure.Interrupted) => None
      case f: Failure if f.isFlagged(Failure.Restartable) => Some(f.show)
      case WriteException(exc) => Some(exc)
      case _ => None
    }
  }

  /**
   * Failures that are generally retryable because the request failed
   * before it finished being written to the remote service.
   * See [[com.twitter.finagle.WriteException]].
   */
  val WriteExceptionsOnly: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(RetryableWriteException(_)) => true
  }

  val TimeoutAndWriteExceptionsOnly: PartialFunction[Try[Nothing], Boolean] =
    WriteExceptionsOnly.orElse {
      case Throw(Failure(Some(_: TimeoutException))) => true
      case Throw(Failure(Some(_: UtilTimeoutException))) => true
      case Throw(_: TimeoutException) => true
      case Throw(_: UtilTimeoutException) => true
    }

  val ChannelClosedExceptionsOnly: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(_: ChannelClosedException) => true
  }

  val Never: RetryPolicy[Try[Nothing]] = new RetryPolicy[Try[Nothing]] {
    def apply(t: Try[Nothing]): Option[(Duration, Nothing)] = None
  }

  /**
   * Converts a `RetryPolicy[Try[Nothing]]` to a `RetryPolicy[(Req, Try[Rep])]`
   * that acts only on exceptions.
   */
  private[finagle] def convertExceptionPolicy[Req, Rep](
    policy: RetryPolicy[Try[Nothing]]
  ): RetryPolicy[(Req, Try[Rep])] =
    new RetryPolicy[(Req, Try[Rep])] {
      def apply(input: (Req, Try[Rep])): Option[(Duration, RetryPolicy[(Req, Try[Rep])])] = input match {
        case (_, t@Throw(_)) =>
          policy(t.asInstanceOf[Throw[Nothing]]) match {
            case Some((howlong, nextPolicy)) => Some((howlong, convertExceptionPolicy(nextPolicy)))
            case None => None
          }
        case (_, Return(_)) => None
      }
    }

  /**
   * Lifts a function of type `A => Option[(Duration, RetryPolicy[A])]` in the  `RetryPolicy` type.
   */
  def apply[A](f: A => Option[(Duration, RetryPolicy[A])]): RetryPolicy[A] =
    new RetryPolicy[A] {
      def apply(e: A): Option[(Duration, RetryPolicy[A])] = f(e)
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
  def tries[A](
    numTries: Int,
    shouldRetry: PartialFunction[A, Boolean]
  ): RetryPolicy[A] = {
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
    backoffs: Stream[Duration]
  )(shouldRetry: PartialFunction[A, Boolean]): RetryPolicy[A] = {
    RetryPolicy { e =>
      if (shouldRetry.applyOrElse(e, AlwaysFalse)) {
        backoffs match {
          case howlong #:: rest =>
            Some((howlong, backoff(rest)(shouldRetry)))
          case _ =>
            None
        }
      } else {
        None
      }
    }
  }

  /**
   * A version of [[backoff]] usable from Java.
   *
   * @param backoffs can be created via [[Backoff.toJava]].
   */
  def backoffJava[A](
    backoffs: juc.Callable[ju.Iterator[Duration]],
    shouldRetry: PartialFunction[A, Boolean]
  ): RetryPolicy[A] = {
    backoff[A](backoffs.call().asScala.toStream)(shouldRetry)
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
    RetryPolicy[A] { e =>
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
