package com.twitter.finagle.service

import com.twitter.finagle.util.Rng
import com.twitter.util.Duration

private[service] object BackoffStrategy {

  /**
   * Create backoffs starting from `start`, and changing by `f`.
   *
   * @param start must be greater than or equal to 0.
   * @param f function to return the next backoff. Please make sure the
   *          returned backoff is non-negative.
   * @note when passing in a referentially transparent function [[f]],
   *       the returned backoff will be calculated based on the current
   *       function that [[f]] refers to.
   */
  def apply(start: Duration, f: Duration => Duration): BackoffStrategy = {
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
  def fromFunction(f: () => Duration): BackoffStrategy = new BackoffFromGeneration(f)

  /**
   * Create backoffs with a constant value `start`.
   *
   * @param start must be greater than or equal to 0.
   */
  def const(start: Duration): BackoffStrategy = {
    require(start >= Duration.Zero)
    new Const(start)
  }

  /**
   * Create backoffs that start at `start`, grow exponentially by `multiplier`.
   *
   * @param start must be greater than or equal to 0.
   * @param multiplier must be greater than 0.
   */
  def exponential(start: Duration, multiplier: Int): BackoffStrategy =
    exponential(start, multiplier, Duration.Top)

  /**
   * Create backoffs that start at `start`, grow exponentially by `multiplier`,
   * and capped at `maximum`.
   *
   * @param start must be greater than or equal to 0.
   * @param multiplier must be greater than 0.
   * @param maximum must be greater than 0.
   */
  def exponential(start: Duration, multiplier: Int, maximum: Duration): BackoffStrategy = {

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
  def linear(start: Duration, offset: Duration): BackoffStrategy =
    linear(start, offset, Duration.Top)

  /**
   * Create backoffs that start at `start`, grows linearly by `offset`, and
   * capped at `maximum`.
   *
   * @param start must be greater than or equal to 0.
   * @param maximum must be greater than or equal to 0.
   */
  def linear(start: Duration, offset: Duration, maximum: Duration): BackoffStrategy = {

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
   * @param maximum maximum must be greater than 0 and greater than or equal to
   *                `start`.
   *
   * @see [[exponentialJittered]] and [[equalJittered]] for alternative
   *      jittered approaches.
   */
  def decorrelatedJittered(start: Duration, maximum: Duration): BackoffStrategy = {

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
   * @param maximum maximum must be greater than 0 and greater than or equal to
   *                `start`.
   *
   * @see [[decorrelatedJittered]] and [[exponentialJittered]] for alternative
   *      jittered approaches.
   */
  def equalJittered(start: Duration, maximum: Duration): BackoffStrategy = {

    require(start > Duration.Zero)
    require(maximum > Duration.Zero)
    require(start <= maximum)

    // compare start and maximum here to avoid one
    // iteration of creating a new `EqualJittered`.
    if (start == maximum) new Const(start)
    else new EqualJittered(start, start, maximum, 1, Rng.threadLocal)
  }

  /**
   * Create backoffs that grow exponentially by 2, capped at `maximum`,
   * with each backoff having jitter, or randomness, between 0 and the
   * exponential backoff value.
   *
   * @param start must be greater than 0 and less than or equal to `maximum`.
   * @param maximum maximum must be greater than 0 and greater than or equal to
   *                `start`.
   *
   * @note This is "full jitter" via
   *       https://www.awsarchitectureblog.com/2015/03/backoff.html
   * @see [[decorrelatedJittered]] and [[equalJittered]] for alternative
   *      jittered approaches.
   */
  def exponentialJittered(start: Duration, maximum: Duration): BackoffStrategy = {

    require(start > Duration.Zero)
    require(maximum > Duration.Zero)
    require(start <= maximum)

    // compare start and maximum here to avoid one
    // iteration of creating a new `ExponentialJittered`.
    if (start == maximum) new Const(start)
    else new ExponentialJittered(start, maximum, 1, Rng.threadLocal)
  }

  /**
   * Stop creating backoffs, this is used to terminate backoff supplies.
   * @note calling `duration` on [[empty]] will return a
   *       [[NoSuchElementException]], calling `next` on this will return an
   *       [[UnsupportedOperationException]].
   */
  val empty: BackoffStrategy = new BackoffStrategy {
    def duration: Duration =
      throw new NoSuchElementException("duration of an empty Backoff")
    def next: BackoffStrategy =
      throw new UnsupportedOperationException("next of an empty Backoff")
    def isExhausted: Boolean = true
  }

  /** @see [[BackoffStrategy.apply]] as the api to create this strategy. */
  private final class BackoffFunction(start: Duration, f: Duration => Duration)
      extends BackoffStrategy {
    def duration: Duration = start
    def next: BackoffStrategy = new BackoffFunction(f(start), f)
    def isExhausted: Boolean = false
  }

  /** @see [[BackoffStrategy.fromFunction]] as the api to create this strategy. */
  private final class BackoffFromGeneration(f: () => Duration) extends BackoffStrategy {
    def duration: Duration = f()
    def next: BackoffStrategy = this
    def isExhausted: Boolean = false
  }

  /** @see [[BackoffStrategy.const]] as the api to create this strategy. */
  private final class Const(start: Duration) extends BackoffStrategy {
    def duration: Duration = start
    def next: BackoffStrategy = this
    def isExhausted: Boolean = false
  }

  /** @see [[BackoffStrategy.exponential]] as the api to create this strategy. */
  private final class Exponential(
    start: Duration,
    multiplier: Int,
    maximum: Duration)
      extends BackoffStrategy {
    def duration: Duration = start
    def next: BackoffStrategy = {
      if (multiplier == 0) new Const(Duration.Zero)
      // in case of Long overflow
      else if (start >= maximum / multiplier) new Const(maximum)
      else new Exponential(start * multiplier, multiplier, maximum)
    }

    def isExhausted: Boolean = false
  }

  /** @see [[BackoffStrategy.linear]] as the api to create this strategy. */
  private final class Linear(start: Duration, offset: Duration, maximum: Duration)
      extends BackoffStrategy {

    def duration: Duration = start
    def next: BackoffStrategy =
      // in case of Long overflow
      if (start >= maximum - offset) new Const(maximum)
      else new Linear(start + offset, offset, maximum)
    def isExhausted: Boolean = false
  }

  /** @see [[BackoffStrategy.decorrelatedJittered]] as the api to create this strategy. */
  // exposed for testing
  private[service] final class DecorrelatedJittered(start: Duration, maximum: Duration, rng: Rng)
      extends BackoffStrategy {
    def duration: Duration = start

    def next: BackoffStrategy = {
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

  /** @see [[BackoffStrategy.equalJittered]] as the api to create this strategy. */
  // exposed for testing
  private[service] final class EqualJittered(
    startDuration: Duration,
    nextDuration: Duration,
    maximum: Duration,
    attempt: Int,
    rng: Rng)
      extends BackoffStrategy {

    // Don't shift left more than 62 bits to avoid
    // Long overflow of the multiplier `shift`.
    private[this] final val MaxBitShift = 62

    def duration: Duration = nextDuration

    def next: BackoffStrategy = {
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

  /** @see [[BackoffStrategy.exponentialJittered]] as the api to create this strategy. */
  // exposed for testing
  private[service] final class ExponentialJittered(
    start: Duration,
    maximum: Duration,
    attempt: Int,
    rng: Rng)
      extends BackoffStrategy {

    // Don't shift left more than 62 bits to avoid
    // Long overflow of the multiplier `shift`.
    private[this] final val MaxBitShift = 62

    def duration: Duration = start

    def next: BackoffStrategy = {
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

  /** @see [[BackoffStrategy.take]] as the api to create this strategy. */
  private final class Take(backoffStrategy: BackoffStrategy, attempt: Int) extends BackoffStrategy {
    def duration: Duration = backoffStrategy.duration
    def next: BackoffStrategy = backoffStrategy.next.take(attempt - 1)

    /**
     * A [[Take]] will never be exhausted. When no backoff will be created,
     * it will return an [[empty]] [[BackoffStrategy]]. This is handled in
     * the implementation of [[take]].
     * @return
     */
    def isExhausted: Boolean = false
  }

  /** @see [[BackoffStrategy.concat]] as the api to create this strategy. */
  private final class Concat(former: BackoffStrategy, latter: BackoffStrategy)
      extends BackoffStrategy {
    def duration: Duration = former.duration
    def next: BackoffStrategy = former.next.concat(latter)

    /**
     * A [[Concat]] will never be exhausted. When no backoff will be created,
     * it will return an [[empty]] [[BackoffStrategy]], instead of a [[Concat]].
     * This only happens when [[latter]] is `empty`, and is handled in the
     * implementation of [[concat]].
     */
    def isExhausted: Boolean = false
  }
}

/**
 * A recursive ADT to define various backoff strategies, where `duration`
 * returns the current backoff, and `next` returns a new [[BackoffStrategy]].
 *
 * All [[BackoffStrategy]]s are infinite unless it [[isExhausted]] (an empty
 * [[BackoffStrategy]]), in this case, calling `duration` will return a
 * [[NoSuchElementException]], calling `next` will return an
 * [[UnsupportedOperationException]].
 *
 * Finagle provides the following backoff strategies:
 *
 * 1. BackoffFunction           - create backoffs based on a given function,
 *                                can be created via `BackoffStrategy.apply`.
 * 2. BackoffFromGeneration     - create backoffs based on a generation
 *                                function, can be created via
 *                                `BackoffStrategy.fromFunction`.
 * 3. Const                     - return a constant backoff, can be created via
 *                                `BackoffStrategy.const`.
 * 4. Exponential               - create backoffs that grow exponentially, can
 *                                be created via `BackoffStrategy.exponential`.
 * 5. Linear                    - create backoffs that grow linearly, can be
 *                                created via `BackoffStrategy.linear`.
 * 6. DecorrelatedJittered      - create backoffs that jitter randomly between
 *                                a start value and 3 times of that value, can
 *                                be created via
 *                                `BackoffStrategy.decorrelatedJittered`.
 * 7. EqualJittered             - create backoffs that jitter between 0 and
 *                                half of the exponential growth. Can be
 *                                created via `BackoffStrategy.equalJittered`.
 * 8. ExponentialJittered       - create backoffs that jitter randomly between
 *                                0 and a value that grows exponentially by 2.
 *                                Can be created via
 *                                `BackoffStrategy.exponentialJittered`.
 *
 * @note A new [[BackoffStrategy]] will be created only when `next` is called.
 * @note None of the [[BackoffStrategy]]s is memoized, for strategies that
 *       involve randomness (`DecorrelatedJittered`, `EqualJittered` and
 *       `ExponentialJittered`), there is no way to foresee the next backoff
 *       value.
 * @note All [[BackoffStrategy]]s are infinite unless using [[take(Int)]] to
 *       create a strategy with limited number of iterations.
 * @note You can combine one or more [[BackoffStrategy]]s with [[take(Int)]]
 *       and [[concat(BackoffStrategy)]].
 * @note If the backoff returned from any [[BackoffStrategy]]s overflowed, all
 *       succeeding backoffs will be [[Duration.Top]].
 */
abstract class BackoffStrategy {
  import BackoffStrategy._

  /** return the current backoff */
  def duration: Duration

  /** return a new BackoffStrategy to get the next backoff */
  def next: BackoffStrategy

  /**
   * return true if this is [[BackoffStrategy.empty]], when true, calling
   * [[duration]] will return a [[NoSuchElementException]], calling `next`
   * will return an [[UnsupportedOperationException]]. This is used to
   * terminate backoff supplies.
   */
  def isExhausted: Boolean

  /**
   * Only create backoffs for `attempt` number of times. When `attempt`
   * is reached, a [[BackoffStrategy.empty]] will be returned.
   *
   * @note [[take(0)]] will return a [[BackoffStrategy.empty]], where
   *       calling `duration` and `next` will throw exceptions.
   */
  def take(attempt: Int): BackoffStrategy = {
    require(attempt >= 0)
    if (attempt == 0 || this.isExhausted) BackoffStrategy.empty
    else new Take(this, attempt)
  }

  /**
   * Start creating backoffs from `that` once the current [[BackoffStrategy]]
   * [[isExhausted]]. You can combine one or more [[BackoffStrategy]]s by:
   *
   * {{{
   *   BackoffStrategy.const(1.second).take(5)
   *     .concat(BackoffStrategy.linear(2.millis, 1.millis).take(7))
   *     .concat(BackoffStrategy.const(9.millis))
   * }}}
   *
   * @note The [[BackoffStrategy]]s are invoked in the same order as they are
   *       concatenated. The former [[BackoffStrategy]] needs to be finite in
   *       order to invoke the succeeding strategies.
   * @see [[take]] on how to create a finite [[BackoffStrategy]].
   */
  def concat(that: BackoffStrategy): BackoffStrategy =
    if (this.isExhausted) that
    else if (that.isExhausted) this
    else new Concat(this, that)
}
