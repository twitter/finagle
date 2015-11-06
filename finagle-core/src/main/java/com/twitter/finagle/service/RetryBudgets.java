package com.twitter.finagle.service;

import com.twitter.util.Duration;
import com.twitter.util.Stopwatch$;

/**
 * Java APIs for {@link RetryBudget}.
 */
public final class RetryBudgets {

  private RetryBudgets() {
    throw new IllegalStateException();
  }

  /**
   * See {@link RetryBudget$#Empty()}
   */
  public static final RetryBudget EMPTY = RetryBudget$.MODULE$.Empty();

  /**
   * See {@link RetryBudget$#Infinite()}
   */
  public static final RetryBudget INFINITE = RetryBudget$.MODULE$.Infinite();

  /**
   * See {@link RetryBudget$#apply()}
   */
  public static RetryBudget newRetryBudget() {
    return RetryBudget$.MODULE$.apply();
  }

  /**
   * See {@link RetryBudget$#apply()}
   */
  public static RetryBudget newRetryBudget(
    Duration ttl,
    int minRetriesPerSec,
    double percentCanRetry
  ) {
    return RetryBudget$.MODULE$.apply(
        ttl,
        minRetriesPerSec,
        percentCanRetry,
        Stopwatch$.MODULE$.systemMillis());
  }

}
