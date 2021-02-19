package com.twitter.finagle.param

import com.twitter.finagle.{Backoff, Stack}
import com.twitter.finagle.service.{Retries, RetryBudget}

/**
 * A collection of methods for basic configuration of Finagle clients.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[https://twitter.github.io/finagle/guide/Clients.html]]
 */
trait ClientParams[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * Configures the [[RetryBudget retry budget]] of this client (default:
   * allows for about 20% of the total requests to be retried on top of
   * 10 retries per second).
   *
   * This `budget` is shared across requests and governs the number of
   * retries that can be made by this client.
   *
   * @note The retry budget helps prevent clients from overwhelming the
   *       downstream service.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#retries]]
   */
  def withRetryBudget(budget: RetryBudget): A =
    self.configured(self.params[Retries.Budget].copy(retryBudget = budget))

  /**
   * Configures the requeue backoff policy of this client (default: no delay).
   *
   * The policy encoded [[Backoff]] is used to calculate the next duration to
   * delay each retry.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#retries]]
   */
  def withRetryBackoff(backoff: Backoff): A =
    self.configured(self.params[Retries.Budget].copy(requeueBackoffs = backoff))
}
