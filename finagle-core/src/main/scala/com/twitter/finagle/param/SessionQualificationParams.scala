package com.twitter.finagle.param

import com.twitter.finagle.{Backoff, Stack}
import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
import com.twitter.finagle.service.FailFastFactory
import com.twitter.util.Duration

/**
 * A collection of methods for configuring modules which help Finagle determine the health
 * of a session. Some of these act as
 * <a href=" https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern">circuit breakers</a>,
 * instructing the load balancer to choose alternate paths. Effectively these enable your
 * client to maintain higher success rates.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
 */
class SessionQualificationParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Disables the Fail Fast module on this client (default: enabled).
   *
   * The Fail Fast module is a Finagle per-session circuit breaker. It marks a host
   * unavailable upon connection failure. The host remains dead until we successfully connect.
   *
   * @note Usually, it's a good idea to disable Fail Fast for server sets with only
   *       one host in the load balancer's replica set.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#fail-fast]]
   *      [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
   *      [[FailFastFactory]]
   */
  def noFailFast: A =
    self.configured(FailFastFactory.FailFast(enabled = false))

  /**
   * Disables the Failure Accrual module on this client (default: enabled).
   *
   * The Failure Accrual module is a Finagle per-request circuit breaker. It marks a
   * host unavailable depending on the used [[FailureAccrualPolicy policy]]. The default
   * setup for the Failure Accrual module is to use a policy based on the number of
   * consecutive failures (default is 5) accompanied by equal jittered backoff
   * producing durations for which a host is marked unavailable.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual]]
   *      [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
   *      [[FailureAccrualFactory]]
   */
  def noFailureAccrual: A =
    self.configured(FailureAccrualFactory.Disabled)

  /**
   * Sets a failure accrual policy that triggers when success rate drops under a given value
   * by looking at a moving average over a time window.
   *
   * The current default policy is success rate based with a cutoff of 80% and
   * a window of 30 seconds.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual]]
   *
   * @param successRate The success rate to trigger on
   * @param window The time window of the moving average
   * @param backoff The [[Backoff]] that should be applied to revival attempts
   * @param minRequestThreshold The minimum number of requests in a window
   * required for failure accrual to trigger
   */
  def successRateFailureAccrual(
    successRate: Double,
    window: Duration,
    backoff: Backoff,
    minRequestThreshold: Int
  ): A = {
    self.configured(
      FailureAccrualFactory.Param(() =>
        FailureAccrualPolicy
          .successRateWithinDuration(successRate, window, backoff, minRequestThreshold))
    )
  }

  /**
   * Sets a failure accrual policy that triggers when success rate drops under a given value
   * by looking at a moving average over a time window.
   *
   * The current default policy is success rate based with a cutoff of 80% and
   * a window of 30 seconds.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual]]
   *
   * @param successRate The success rate to trigger on
   * @param window The time window of the moving average
   */
  def successRateFailureAccrual(successRate: Double, window: Duration): A =
    successRateFailureAccrual(
      successRate,
      window,
      FailureAccrualFactory.jitteredBackoff,
      FailureAccrualPolicy.DefaultMinimumRequestThreshold
    )

  /**
   * Sets a failure accrual policy that triggers when success rate drops under a given value
   * by looking at a moving average over a time window.
   *
   * The current default policy is success rate based with a cutoff of 80% and
   * a window of 30 seconds.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual]]
   *
   * @param successRate The success rate to trigger on
   * @param window The time window of the moving average
   * @param backoff The [[Backoff]] that should be applied to revival attempts
   */
  def successRateFailureAccrual(
    successRate: Double,
    window: Duration,
    backoff: Backoff
  ): A =
    successRateFailureAccrual(
      successRate,
      window,
      backoff,
      FailureAccrualPolicy.DefaultMinimumRequestThreshold
    )

  /**
   * Sets a FailureAccrualPolicy that triggers after `nFailures` consecutive failures.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual]]
   *      [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
   *      [[FailureAccrualFactory]]
   *
   * @param nFailures The number of failures to trigger on
   * @param backoff The [[Backoff]] that should be applied to revival attempts
   */
  def consecutiveFailuresFailureAccrual(nFailures: Int, backoff: Backoff): A = {
    self.configured(
      FailureAccrualFactory.Param(() =>
        FailureAccrualPolicy.consecutiveFailures(nFailures, backoff))
    )
  }

  /**
   * Sets a FailureAccrualPolicy that triggers after `nFailures` consecutive failures.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual]]
   *      [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
   *      [[FailureAccrualFactory]]
   *
   * @param nFailures The number of failures to trigger on
   */
  def consecutiveFailuresFailureAccrual(nFailures: Int): A =
    consecutiveFailuresFailureAccrual(nFailures, FailureAccrualFactory.jitteredBackoff)
}
