package com.twitter.finagle.service.exp;

import com.twitter.finagle.service.Backoff;
import com.twitter.util.Duration;

/**
 * Java APIs for {@link FailureAccrualPolicy}.
 */
public final class FailureAccrualPolicies {

  private FailureAccrualPolicies() {
    throw new IllegalStateException();
  }

  /**
   * A policy based on an exponentially-weighted moving average success rate
   * over a window of requests. A moving average is used so the success rate
   * calculation is biased towards more recent requests; for an endpoint with
   * low traffic, the window will span a longer time period and early successes
   * and failures may not accurately reflect the current health.
   *
   * If the computed weighted success rate is less
   * than the required success rate, `markDeadOnFailure()` will return
   * Some(Duration).
   *
   * @see com.twitter.finagle.util.Ema for how the succes rate is computed
   *
   * @param requiredSuccessRate successRate that must be met
   *
   * @param window window over which the success rate is tracked. `window` requests
   * must occur for `markDeadOnFailure()` to ever return Some(Duration)
   *
   * @param markDeadFor Duration returned from `markDeadOnFailure()`
   */
  public static FailureAccrualPolicy newSuccessRatePolicy(
    double requiredSuccessRate,
    int window,
    Duration markDeadFor
  ) {
    return FailureAccrualPolicy$.MODULE$.successRate(
        requiredSuccessRate,
        window,
        Backoff.constant(markDeadFor));
  }

  /**
   * A policy based on a maximum number of consecutive failures. If `numFailures`
   * occur consecutively, `checkFailures()` will return a Some(Duration) to
   * mark an endpoint dead for.
   *
   * @param numFailures number of consecutive failures
   *
   * @param markDeadFor Duration returned from `markDeadOnFailure()`
   */
  public static FailureAccrualPolicy newConsecutiveFailuresPolicy(
    int numFailures,
    Duration markDeadFor
  ) {
    return FailureAccrualPolicy$.MODULE$.consecutiveFailures(
        numFailures,
        Backoff.constant(markDeadFor));
  }
}
