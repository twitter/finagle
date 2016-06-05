package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.util.{Duration, Stopwatch, TokenBucket}

/**
 * Represents a budget for retrying requests.
 *
 * A retry budget is useful for attenuating the amplifying effects
 * of many clients within a process retrying requests multiple
 * times. This acts as a form of coordination between those retries.
 *
 * Implementations must be thread-safe.
 *
 * @see [[RetryBudget.apply]] for creating instances.
 *
 * @see [[Retries]] for how budgets get translated into
 * [[com.twitter.finagle.Filter Filters]].
 */
trait RetryBudget {
  /**
   * Indicates a deposit, or credit, which will typically
   * permit future withdrawals.
   */
  def deposit(): Unit

  /**
   * Check whether or not there is enough balance remaining
   * to issue a retry, or make a withdrawal.
   *
   * If `true`, if the retry is allowed and a withdrawal will take
   * place.
   * If `false`, the balance should remain untouched.
   */
  def tryWithdraw(): Boolean

  /**
   * The balance or number of retries that can be made now.
   */
  def balance: Long
}

/**
 * See [[RetryBudgets]] for Java APIs.
 */
object RetryBudget {

  /**
   * An immutable [[RetryBudget]] that never has a balance,
   * and as such, will never allow a retry.
   */
  val Empty: RetryBudget = new RetryBudget {
    def deposit(): Unit = ()
    def tryWithdraw(): Boolean = false
    def balance: Long = 0L
  }

  /**
   * An immutable [[RetryBudget]] that always has a balance of `100`,
   * and as such, will always allow a retry.
   */
  val Infinite: RetryBudget = new RetryBudget {
    def deposit(): Unit = ()
    def tryWithdraw(): Boolean = true
    def balance: Long = 100L
  }


  private object TokenRetryBudget {
    /**
     * This scaling factor allows for `percentCanRetry` > 1 without
     * having to use floating points (as the underlying mechanism
     * here is a `TokenBucket` which is not floating point based).
     */
    val ScaleFactor = 1000.0
  }

  private class TokenRetryBudget(
      tokenBucket: TokenBucket,
      depositAmount: Int,
      withdrawalAmount: Int)
    extends RetryBudget
  {
    def deposit(): Unit =
      tokenBucket.put(depositAmount)

    def tryWithdraw(): Boolean =
      tokenBucket.tryGet(withdrawalAmount)

    def balance: Long =
      tokenBucket.count / withdrawalAmount
  }

  private[this] val DefaultTtl = 10.seconds

  /**
   * Creates a default [[RetryBudget]] that allows for about 20% of the
   * total [[RetryBudget.deposit() requests]] to be retried on top of
   * a minimum number per second in order to accommodate clients that
   * have just started issuing requests or clients that have a low
   * rate of requests per second.
   *
   * Deposits created by `deposit()` expire after some amount of time.
   * There is also a minimum reserve of retries allowed per time period
   * in order to accommodate clients that have just started issuing
   * requests as well as clients that do not issue many requests per window.
   */
  def apply(): RetryBudget =
    apply(DefaultTtl, 10, 0.2, Stopwatch.systemMillis)

  /**
   * Creates a [[RetryBudget]] that allows for about `percentCanRetry` percent
   * of the total [[RetryBudget.deposit() requests]] to be retried.
   *
   * @param ttl Deposits created by `deposit()` expire after
   * approximately `ttl` time has passed. Must be `>= 1 second`
   * and `<= 60 seconds`.
   *
   * @param minRetriesPerSec the minimum rate of retries allowed in order to
   * accommodate clients that have just started issuing requests as well as clients
   * that do not issue many requests per window.
   * Must be non-negative and if `0`, then no reserve is given.
   *
   * @param percentCanRetry the percentage of calls to `deposit()` that can be
   * retried. This is in addition to any retries allowed for via `minRetriesPerSec`.
   * Must be >= 0 and <= 1000. As an example, if `0.1` is used, then for every
   * 10 calls to `deposit()`, 1 retry will be allowed. If `2.0` is used then every
   * `deposit` allows for 2 retries.
   *
   * @param nowMillis the current time in milliseconds since the epoch.
   * The default of [[Stopwatch.systemMillis]] is generally appropriate,
   * though using [[Stopwatch.timeMillis]] is useful for well behaved tests
   * so that you can control [[com.twitter.util.Time]].
   */
  def apply(
    ttl: Duration,
    minRetriesPerSec: Int,
    percentCanRetry: Double,
    nowMillis: () => Long = Stopwatch.systemMillis
  ): RetryBudget = {
    require(ttl.inSeconds >= 1 && ttl.inSeconds <= 60,
      s"ttl must be [1 second, 60 seconds]: $ttl")
    require(minRetriesPerSec >= 0,
      s"minRetriesPerSec must be non-negative: $minRetriesPerSec")
    require(percentCanRetry >= 0.0,
      s"percentCanRetry must be non-negative: $percentCanRetry")
    require(percentCanRetry <= TokenRetryBudget.ScaleFactor,
      s"percentCanRetry must not be greater than ${TokenRetryBudget.ScaleFactor}: $percentCanRetry")

    if (minRetriesPerSec == 0 && percentCanRetry == 0.0)
      return Empty

    // if you only have minRetries, everything costs 1 but you
    // get no credit for requests. all credits come via time.
    val depositAmount =
      if (percentCanRetry == 0.0) 0
      else TokenRetryBudget.ScaleFactor.toInt
    val withdrawalAmount =
      if (percentCanRetry == 0.0) 1
      else (TokenRetryBudget.ScaleFactor / percentCanRetry).toInt

    // compute the reserve by scaling minRetriesPerSec by ttl and retry cost
    // to allow for clients that've just started or have low rps
    val reserve = minRetriesPerSec * ttl.inSeconds * withdrawalAmount

    val tokenBucket = TokenBucket.newLeakyBucket(ttl, reserve, nowMillis)
    new TokenRetryBudget(tokenBucket, depositAmount, withdrawalAmount)
  }

}
