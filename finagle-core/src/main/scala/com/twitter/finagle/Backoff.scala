package com.twitter.finagle

import com.twitter.finagle.util.Rng
import com.twitter.util.Duration
import java.{util => ju}
import scala.collection.JavaConverters._

object Backoff {

  /**
   * Create a [[Backoff]] from a [[Stream[Duration]].
   * @note This API is only recommended to be used for migrating from
   *       [[Stream]] to [[Backoff]].
   * @note Every element of `s` will be memorized due to the nature of
   *       [[Stream]], when using this method, your service is at risk
   *       of memory leaks.
   */
  def fromStream(s: Stream[Duration]): Backoff = new FromStream(s)

  /**
   * Create backoffs starting from `start`, and changing by `f`.
   *
   * @param start must be greater than or equal to 0.
   * @param f function to return the next backoff. Please make sure the
   *          returned backoff is non-negative.
   * @note when passing in a referentially transparent function `f`,
   *       the returned backoff will be calculated based on the current
   *       function that `f` refers to.
   */
  def apply(start: Duration)(f: Duration => Duration): Backoff = {
    require(start >= Duration.Zero)
    new BackoffFunction(start, f)
  }

  /**
   * Create backoffs with values produced by a given generation function.
   *
   * @note `duration` is computed for each occurrence. This is similar to
   *       [[Stream.continually]]. It is not guaranteed that calling
   *       `duration` on the same strategies multiple times returns the
   *       same value.
   * @note please make sure the backoffs returned from the generation
   *       function is non-negative.
   */
  def fromFunction(f: () => Duration): Backoff = new BackoffFromGeneration(f)

  /**
   * Create backoffs with a constant value `start`.
   *
   * @param start must be greater than or equal to 0.
   *
   * @see [[constant]] for a Java friendly API
   */
  def const(start: Duration): Backoff = {
    require(start >= Duration.Zero)
    new Const(start)
  }

  /** Alias for [[const]], which is a reserved word in Java */
  def constant(start: Duration): Backoff = const(start)

  /**
   * Create backoffs that start at `start`, grow exponentially by `multiplier`.
   *
   * @param start must be greater than or equal to 0.
   * @param multiplier must be greater than 0.
   */
  def exponential(start: Duration, multiplier: Int): Backoff =
    exponential(start, multiplier, Duration.Top)

  /**
   * Create backoffs that start at `start`, grow exponentially by `multiplier`,
   * and capped at `maximum`.
   *
   * @param start must be greater than or equal to 0.
   * @param multiplier must be greater than 0.
   * @param maximum must be greater than 0.
   */
  def exponential(start: Duration, multiplier: Int, maximum: Duration): Backoff = {

    require(start >= Duration.Zero)
    require(multiplier >= 0)
    require(maximum >= Duration.Zero)

    if (multiplier == 1) new Const(start)
    else if (start >= maximum) new Const(maximum)
    else new Exponential(start, multiplier, maximum)
  }

  /**
   * Create backoffs that start at `start`, grows linearly by `offset`.
   *
   * @param start must be greater than or equal to 0.
   */
  def linear(start: Duration, offset: Duration): Backoff =
    linear(start, offset, Duration.Top)

  /**
   * Create backoffs that start at `start`, grows linearly by `offset`, and
   * capped at `maximum`.
   *
   * @param start must be greater than or equal to 0.
   * @param maximum must be greater than or equal to 0.
   */
  def linear(start: Duration, offset: Duration, maximum: Duration): Backoff = {

    require(start >= Duration.Zero)
    require(maximum >= Duration.Zero)

    if (offset == Duration.Zero) new Const(start)
    else if (start >= maximum) new Const(maximum)
    else new Linear(start, offset, maximum)
  }

  /**
   * Create backoffs that have jitter with a random distribution between
   * `start` and 3 times the previously selected value, capped at `maximum`.
   *
   * @param start must be greater than 0 and less than or equal to `maximum`.
   * @param maximum must be greater than 0 and greater than or equal to
   *                `start`.
   *
   * @see [[exponentialJittered]] and [[equalJittered]] for alternative
   *      jittered approaches.
   */
  def decorrelatedJittered(start: Duration, maximum: Duration): Backoff = {

    require(start > Duration.Zero)
    require(maximum > Duration.Zero)
    require(start <= maximum)

    // compare start and maximum here to avoid one
    // iteration of creating a new `DecorrelatedJittered`.
    if (start == maximum) new Const(start)
    else new DecorrelatedJittered(start, maximum, Rng.threadLocal)
  }

  /**
   * Create backoffs that keep half of the exponential growth, and jitter
   * between 0 and that amount.
   *
   * @param start must be greater than 0 and less than or equal to `maximum`.
   * @param maximum must be greater than 0 and greater than or equal to
   *                `start`.
   *
   * @see [[decorrelatedJittered]] and [[exponentialJittered]] for alternative
   *      jittered approaches.
   */
  def equalJittered(start: Duration, maximum: Duration): Backoff = {

    require(start > Duration.Zero)
    require(maximum > Duration.Zero)
    require(start <= maximum)

    // compare start and maximum here to avoid one
    // iteration of creating a new `EqualJittered`.
    if (start == maximum) new Const(start)
    else new EqualJittered(start, start, maximum, 1, Rng.threadLocal)
  }

  /**
   * The formula to calculate the next backoff is:
   *{{{
   *   backoff = random_between(0, min({@code maximum}, {@code start} * 2 ** attempt))
   *}}}
   *
   * @param start must be greater than 0 and less than or equal to `maximum`.
   * @param maximum must be greater than 0 and greater than or equal to
   *                `start`.
   *
   * @note This is "full jitter" via
   *       https://www.awsarchitectureblog.com/2015/03/backoff.html
   * @see [[decorrelatedJittered]] and [[equalJittered]] for alternative
   *      jittered approaches.
   */
  def exponentialJittered(start: Duration, maximum: Duration): Backoff = {

    require(start > Duration.Zero)
    require(maximum > Duration.Zero)
    require(start <= maximum)

    // compare start and maximum here to avoid one
    // iteration of creating a new `ExponentialJittered`.
    if (start == maximum) new Const(start)
    else new ExponentialJittered(start, maximum, 1, Rng.threadLocal)
  }

  /**
   * @deprecated Use any [[Backoff]] api to generate java friendly backoffs.
   *             This api is kept for backward compatible and will be removed.
   */
  def toJava(backoff: Backoff): Backoff = backoff

  /**
   * Stop creating backoffs, this is used to terminate backoff supplies.
   * @note calling `duration` on [[empty]] will return a
   *       [[NoSuchElementException]], calling `next` on this will return an
   *       [[UnsupportedOperationException]].
   */
  val empty: Backoff = new Backoff {
    def duration: Duration =
      throw new NoSuchElementException("duration of empty Backoff")
    def next: Backoff =
      throw new UnsupportedOperationException("next of empty Backoff")
    def isExhausted: Boolean = true
  }

  /** @see [[Backoff.apply]] as the api to create this strategy. */
  private final class BackoffFunction(start: Duration, f: Duration => Duration) extends Backoff {
    def duration: Duration = start
    def next: Backoff = new BackoffFunction(f(start), f)
    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.fromFunction]] as the api to create this strategy. */
  private final class BackoffFromGeneration(f: () => Duration) extends Backoff {
    def duration: Duration = f()
    def next: Backoff = this
    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.const]] as the api to create this strategy. */
  private final class Const(start: Duration) extends Backoff {
    def duration: Duration = start
    def next: Backoff = this
    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.exponential]] as the api to create this strategy. */
  private final class Exponential(
    start: Duration,
    multiplier: Int,
    maximum: Duration)
      extends Backoff {
    def duration: Duration = start
    def next: Backoff = {
      if (multiplier == 0) new Const(Duration.Zero)
      // in case of Long overflow
      else if (start >= maximum / multiplier) new Const(maximum)
      else new Exponential(start * multiplier, multiplier, maximum)
    }

    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.linear]] as the api to create this strategy. */
  private final class Linear(start: Duration, offset: Duration, maximum: Duration) extends Backoff {

    def duration: Duration = start
    def next: Backoff =
      // in case of Long overflow
      if (start >= maximum - offset) new Const(maximum)
      else new Linear(start + offset, offset, maximum)
    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.decorrelatedJittered]] as the api to create this strategy. */
  // exposed for testing
  private[finagle] final class DecorrelatedJittered(start: Duration, maximum: Duration, rng: Rng)
      extends Backoff {
    def duration: Duration = start

    def next: Backoff = {
      // in case of Long overflow
      val upperbound = if (start >= maximum / 3) maximum else start * 3
      val randRange = math.abs(upperbound.inNanoseconds - start.inNanoseconds)
      val randBackoff =
        if (randRange == 0) start.inNanoseconds
        else start.inNanoseconds + rng.nextLong(randRange)
      val nextStart = Duration.fromNanoseconds(randBackoff)

      if (nextStart == maximum) new Const(maximum)
      else new DecorrelatedJittered(nextStart, maximum, rng)
    }

    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.equalJittered]] as the api to create this strategy. */
  // exposed for testing
  private[finagle] final class EqualJittered(
    startDuration: Duration,
    nextDuration: Duration,
    maximum: Duration,
    attempt: Int,
    rng: Rng)
      extends Backoff {

    // Don't shift left more than 62 bits to avoid
    // Long overflow of the multiplier `shift`.
    private[this] final val MaxBitShift = 62

    def duration: Duration = nextDuration

    def next: Backoff = {
      val shift = 1L << MaxBitShift.min(attempt - 1)
      // in case of Long overflow
      val halfExp = if (startDuration >= maximum / shift) maximum else startDuration * shift
      val randomBackoff = Duration.fromNanoseconds(rng.nextLong(halfExp.inNanoseconds))

      // in case of Long overflow
      if (halfExp == maximum || halfExp >= maximum - randomBackoff) new Const(maximum)
      else new EqualJittered(startDuration, halfExp + randomBackoff, maximum, attempt + 1, rng)
    }

    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.exponentialJittered]] as the api to create this strategy. */
  // exposed for testing
  private[finagle] final class ExponentialJittered(
    start: Duration,
    maximum: Duration,
    attempt: Int,
    rng: Rng)
      extends Backoff {

    // Don't shift left more than 62 bits to avoid
    // Long overflow of the multiplier `shift`.
    private[this] final val MaxBitShift = 62

    def duration: Duration = start

    def next: Backoff = {
      val shift = 1L << MaxBitShift.min(attempt)
      // to avoid Long overflow
      val maxBackoff = if (start >= maximum / shift) maximum else start * shift
      if (maxBackoff == maximum) {
        new Const(maximum)
      } else {
        val random = Duration.fromNanoseconds(rng.nextLong(maxBackoff.inNanoseconds))
        new ExponentialJittered(random, maximum, attempt + 1, rng)
      }
    }

    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.take]] as the api to create this strategy. */
  private final class Take(backoff: Backoff, attempt: Int) extends Backoff {
    def duration: Duration = backoff.duration
    def next: Backoff = backoff.next.take(attempt - 1)

    /**
     * A [[Take]] will never be exhausted. When no backoff will be created,
     * it will return an [[empty]] [[Backoff]]. This is handled in
     * the implementation of [[take]].
     *
     * @return
     */
    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.takeUntil]] as the api to create this strategy. */
  class TakeWhile(backoff: Backoff, count: Duration, maxCumulativeBackoff: Duration)
      extends Backoff {
    def duration: Duration =
      if (count < maxCumulativeBackoff) backoff.duration
      else Backoff.empty.duration
    def next: Backoff =
      if (count < maxCumulativeBackoff)
        new TakeWhile(backoff.next, count + duration, maxCumulativeBackoff)
      else Backoff.empty.next
    def isExhausted: Boolean = if (count < maxCumulativeBackoff) false else true
  }

  /** @see [[Backoff.concat]] as the api to create this strategy. */
  private final class Concat(former: Backoff, latter: Backoff) extends Backoff {
    def duration: Duration = former.duration
    def next: Backoff = former.next.concat(latter)

    /**
     * A [[Concat]] will never be exhausted. When no backoff will be created,
     * it will return an [[empty]] [[Backoff]], instead of a [[Concat]].
     * This only happens when [[latter]] is `empty`, and is handled in the
     * implementation of [[concat]].
     */
    def isExhausted: Boolean = false
  }

  /** @see [[Backoff.fromStream]] as the api to create this strategy. */
  private final class FromStream(s: Stream[Duration]) extends Backoff {
    def duration: Duration = if (s.isEmpty) Backoff.empty.duration else s.head
    def next: Backoff =
      if (s.isEmpty) Backoff.empty.next
      else fromStream(s.tail)
    def isExhausted: Boolean = s.isEmpty
  }
}

/**
 * A recursive ADT to define various backoff strategies, where `duration`
 * returns the current backoff, and `next` returns a new [[Backoff]].
 *
 * All [[Backoff]]s are infinite unless it [[isExhausted]] (an empty
 * [[Backoff]]), in this case, calling `duration` will return a
 * [[NoSuchElementException]], calling `next` will return an
 * [[UnsupportedOperationException]].
 *
 * Finagle provides the following backoff strategies:
 *
 *   1. BackoffFunction
 *     - Create backoffs based on a given function, can be created via `Backoff.apply`.
 *   1. BackoffFromGeneration
 *     - Create backoffs based on a generation function, can be created via `Backoff.fromFunction`.
 *   1. Const
 *     - Return a constant backoff, can be created via `Backoff.const`.
 *   1. Exponential
 *     - Create backoffs that grow exponentially, can be created via `Backoff.exponential`.
 *   1. Linear
 *     - Create backoffs that grow linearly, can be created via `Backoff.linear`.
 *   1. DecorrelatedJittered
 *     - Create backoffs that jitter randomly between a start value and 3 times of that value, can be created via `Backoff.decorrelatedJittered`.
 *   1. EqualJittered
 *     - Create backoffs that jitter between 0 and half of the exponential growth. Can be created via `Backoff.equalJittered`.
 *   1. ExponentialJittered
 *     - Create backoffs that jitter randomly between 0 and a value that grows exponentially by 2. Can be created via `Backoff.exponentialJittered`.
 *
 * @note A new [[Backoff]] will be created only when `next` is called.
 * @note None of the [[Backoff]]s are memoized, for strategies that involve
 *       randomness (`DecorrelatedJittered`, `EqualJittered` and
 *       `ExponentialJittered`), there is no way to foresee the next backoff
 *       value.
 * @note All [[Backoff]]s are infinite unless using [[take(Int)]] to create a
 *       strategy with limited number of iterations.
 * @note You can combine one or more [[Backoff]]s with [[take(Int)]] and
 *       [[concat(Backoff)]].
 * @note If the backoff returned from any [[Backoff]]s overflowed, all
 *       succeeding backoffs will be [[Duration.Top]].
 */
sealed abstract class Backoff private {
  import Backoff._

  override def toString: String = s"${getClass.getName}"

  /** return the current backoff */
  def duration: Duration

  /** return a new Backoff to get the next backoff */
  def next: Backoff

  /**
   * return true if this is [[Backoff.empty]], when true, calling
   * [[duration]] will return a [[NoSuchElementException]], calling `next`
   * will return an [[UnsupportedOperationException]]. This is used to
   * terminate backoff supplies.
   */
  def isExhausted: Boolean

  /**
   * Only create backoffs for `attempt` number of times. When `attempt`
   * is reached, a [[Backoff.empty]] will be returned.
   *
   * @note Calling [[take]] with a non-positive `attempt` will return a
   *       [[Backoff.empty]], where calling `duration` and `next` will
   *       throw exceptions.
   */
  final def take(attempt: Int): Backoff = {
    if (attempt <= 0 || this.isExhausted) Backoff.empty
    else new Take(this, attempt)
  }

  /**
   * Only create backoffs until the sum of all previous backoffs is less than
   * or equal to `maxCumulativeBackoff`. When `maxCumulativeBackoff` is
   * reached, a [[Backoff.empty]] will be returned.
   *
   * @note Calling [[takeUntil]] with a non-positive `maxCumulativeBackoff`
   *       will return a [[Backoff.empty]], where calling `duration` and `next`
   *       will throw exceptions.
   */
  final def takeUntil(maxCumulativeBackoff: Duration): Backoff = {
    if (maxCumulativeBackoff <= Duration.Zero || this.isExhausted) Backoff.empty
    else new TakeWhile(this, Duration.Zero, maxCumulativeBackoff)
  }

  /**
   * Start creating backoffs from `that` once the current [[Backoff]]
   * [[isExhausted]]. You can combine one or more [[Backoff]]s by:
   *
   * {{{
   *   Backoff.const(1.second).take(5)
   *     .concat(Backoff.linear(2.millis, 1.millis).take(7))
   *     .concat(Backoff.const(9.millis))
   * }}}
   *
   * @note The [[Backoff]]s are invoked in the same order as they are
   *       concatenated. The former [[Backoff]] needs to be finite in
   *       order to invoke the succeeding strategies.
   * @see [[take]] on how to create a finite [[Backoff]].
   */
  final def concat(that: Backoff): Backoff =
    if (this.isExhausted) that
    else if (that.isExhausted) this
    else new Concat(this, that)

  /**
   * An alias for Backoff concatenation.
   */
  final def ++(that: Backoff): Backoff = concat(that)

  /**
   * Convert the [[Backoff]] to a [[Stream[Duration]]]
   *
   * @note This API is only recommended to be used for migrating from
   *       [[Stream]] to [[Backoff]].
   * @note Every element of the returned `Stream` will be memorized due
   *       to the nature of [[Stream]], when using this method, your service
   *       is at risk of memory leaks.
   */
  final def toStream: Stream[Duration] =
    if (isExhausted) Stream.empty[Duration]
    else duration #:: next.toStream

  /**
   * Convert a [[Backoff]] into a Java-friendly representation of iterator.
   */
  final def toJavaIterator: ju.Iterator[Duration] = toStream.toIterator.asJava
}
