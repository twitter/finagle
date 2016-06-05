package com.twitter.finagle.service

import com.twitter.finagle.util.Rng
import com.twitter.util.Duration
import java.util.{concurrent => juc}
import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * Implements various backoff strategies.
 *
 * Strategies are defined by a `Stream[Duration]` and are intended for use with
 * [[RetryFilter.apply]] and [[RetryPolicy.backoff]] to determine the duration after which a request
 * is to be retried.
 *
 * @note All backoffs created by factory methods on this object are infinite. Use `Stream.take` to
 *       make them terminate.
 */
object Backoff {

  /**
   * This is a smarter version of [[Stream.iterate]] in the way that it goes to
   * [[Backoff.const]] (to save allocations) as long as `f` doesn't change its input.
   */
  private[this] def tailless(start: Duration)(f: Duration => Duration): Stream[Duration] = {
    val next = f(start)
    start #:: (if (next == start) const(next) else tailless(next)(f))
  }

  /**
   * Create infinite backoffs that start with `start` and change by `f`.
   *
   * @note This is an exact version of [[Stream.iterate]].
   */
  def apply(start: Duration)(f: Duration => Duration): Stream[Duration] = Stream.iterate(start)(f)

  /**
   * Create infinite backoffs that grow exponentially by `multiplier`.
   *
   * @see [[exponentialJittered]] for a version that incorporates jitter.
   */
  def exponential(start: Duration, multiplier: Int): Stream[Duration] =
    exponential(start, multiplier, Duration.Top)

  /**
   * Create infinite backoffs that grow exponentially by `multiplier`, capped at `maximum`.
   *
   * @see [[exponentialJittered]] for a version that incorporates jitter.
   */
  def exponential(start: Duration, multiplier: Int, maximum: Duration): Stream[Duration] =
    tailless(start)(prev => maximum.min(prev * multiplier))

  /**
   * Create infinite backoffs that grow exponentially by 2, capped at `maximum`,
   * with each backoff having jitter, or randomness, between 0 and the
   * exponential backoff value.
   *
   * @param start must be greater than 0 and less than or equal to `maximum`.
   * @param maximum must be greater than 0 and greater than or equal to `start`.
   * @see [[decorrelatedJittered]] and [[equalJittered]] for alternative jittered approaches.
   */
  def exponentialJittered(start: Duration, maximum: Duration): Stream[Duration] =
    exponentialJittered(start, maximum, Rng.threadLocal)

  // Don't shift left more than 62 bits to avoid Long overflow.
  private[this] val MaxBitShift = 62

  /** Exposed for testing */
  private[service] def exponentialJittered(
    start: Duration,
    maximum: Duration,
    rng: Rng
  ): Stream[Duration] = {
    require(start > Duration.Zero)
    require(maximum > Duration.Zero)
    require(start <= maximum)
    // this is "full jitter" via http://www.awsarchitectureblog.com/2015/03/backoff.html
    def next(attempt: Int): Stream[Duration] = {
      val shift = math.min(attempt, MaxBitShift)
      val maxBackoff = maximum.min(start * (1L << shift))
      val random = Duration.fromNanoseconds(rng.nextLong(maxBackoff.inNanoseconds))
      random #:: next(attempt + 1)
    }
    start #:: next(1)
  }

  /**
   * Create infinite backoffs that have jitter with a random distribution
   * between `start `and 3 times the previously selected value, capped at `maximum`.
   *
   * @param start must be greater than 0 and less than or equal to `maximum`.
   * @param maximum must be greater than 0 and greater than or equal to `start`.
   * @see [[exponentialJittered]] and [[equalJittered]] for alternative jittered approaches.
   */
  def decorrelatedJittered(start: Duration, maximum: Duration): Stream[Duration] =
    decorrelatedJittered(start, maximum, Rng.threadLocal)

  /** Exposed for testing */
  private[service] def decorrelatedJittered(
    start: Duration,
    maximum: Duration,
    rng: Rng
  ): Stream[Duration] = {
    require(start > Duration.Zero)
    require(maximum > Duration.Zero)
    require(start <= maximum)

    // this is "decorrelated jitter" via http://www.awsarchitectureblog.com/2015/03/backoff.html
    def next(prev: Duration): Stream[Duration] = {
      val randRange = math.abs((prev.inNanoseconds * 3) - start.inNanoseconds)
      val randBackoff =
        if (randRange == 0) start.inNanoseconds
        else start.inNanoseconds + rng.nextLong(randRange)

      val backoffNanos = math.min(maximum.inNanoseconds, randBackoff)
      val backoff = Duration.fromNanoseconds(backoffNanos)
      backoff #:: next(backoff)
    }
    start #:: next(start)
  }

  /**
   * Create infinite backoffs that keep half of the exponential growth, and jitter
   * between 0 and that amount.
   *
   * @see [[exponentialJittered]] and [[decorrelatedJittered]] for alternative jittered approaches.
   */
  def equalJittered(start: Duration, maximum: Duration): Stream[Duration] =
    equalJittered(start, maximum, Rng.threadLocal)

  /** Exposed for testing */
  private[service] def equalJittered(
    start: Duration,
    maximum: Duration,
    rng: Rng = Rng.threadLocal
  ): Stream[Duration] = {
    require(start > Duration.Zero)
    require(maximum > Duration.Zero)
    require(start <= maximum)
    // this is "equal jitter" via http://www.awsarchitectureblog.com/2015/03/backoff.html
    def next(attempt: Int): Stream[Duration] = {
      val shift = math.min(attempt - 1, MaxBitShift)
      val halfExp = start * (1L << shift)
      val backoff = halfExp + Duration.fromNanoseconds(rng.nextLong(halfExp.inNanoseconds))
      if (backoff < maximum) backoff #:: next(attempt + 1)
      else const(maximum)
    }
    start #:: next(1)
  }

  /**
   * Create infinite backoffs that grow linear by `offset`.
   */
  def linear(start: Duration, offset: Duration): Stream[Duration] =
    linear(start, offset, Duration.Top)

  /**
   * Create infinite backoffs that grow linear by `offset`, capped at `maximum`.
   */
  def linear(start: Duration, offset: Duration, maximum: Duration): Stream[Duration] =
    tailless(start)(prev => maximum.min(prev + offset))

  /** Alias for [[const]], which is a reserved word in Java */
  def constant(start: Duration): Stream[Duration] = const(start)

  /** See [[constant]] for a Java friendly API */
  def const(start: Duration): Stream[Duration] = {
    // We don't want to allocate a new cons on each element in the infinite
    // stream as it's done in Stream.continually so we reuse it.
    lazy val self: Stream[Duration] = Stream.cons(start, self)
    self
  }

  /**
   * Create infinite backoffs with values produced by a given generation function.
   *
   * @note This is an exact version of [[Stream.continually]].
   */
  def fromFunction(f: () => Duration): Stream[Duration] = Stream.continually(f())

  /**
   * Convert a [[Stream]] of [[Duration Durations]] into a Java-friendly representation.
   */
  def toJava(backoffs: Stream[Duration]): juc.Callable[ju.Iterator[Duration]] = {
    new ju.concurrent.Callable[ju.Iterator[Duration]] {
      def call(): ju.Iterator[Duration] = backoffs.toIterator.asJava
    }
  }
}
