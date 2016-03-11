package com.twitter.finagle.param

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.Stack
import com.twitter.finagle.filter.RequestSemaphoreFilter
import com.twitter.finagle.service.DeadlineFilter
import com.twitter.util.Duration

/**
 * A collection of methods for configuring the server-side admission control modules
 * of Finagle servers.
 *
 * @tparam A a [[Stack.Parameterized]] server to configure
 */
class ServerAdmissionControlParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Configures the requests concurrency of this server.
   *
   * @param maxConcurrentRequests the maximum number of requests allowed to be handled
   *                              concurrently (default: unbounded)
   *
   * @param maxWaiters the maximum number requests (on top of `maxConcurrentRequests`)
   *                   allowed to be queued (default: unbounded)
   *
   * @see [[https://twitter.github.io/finagle/guide/Servers.html#concurrency-limit]]
   */
  def concurrencyLimit(maxConcurrentRequests: Int, maxWaiters: Int): A = {
    val semaphore =
      if (maxConcurrentRequests == Int.MaxValue) None
      else Some(new AsyncSemaphore(maxConcurrentRequests, maxWaiters))

    self.configured(RequestSemaphoreFilter.Param(semaphore))
  }

  /**
   * Configures the request deadline `tolerance` of this server.
   *
   * Helps servers avoid doing unnecessary work that is already past its deadline.
   *
   * @param tolerance the maximum elapsed time since a request's deadline when it
   *                  will be considered for rejection (default: 170 ms)
   *
   * @see [[https://twitter.github.io/finagle/guide/Servers.html#request-deadline]]
   */
  def deadlineTolerance(tolerance: Duration): A =
    self.configured(self.params[DeadlineFilter.Param].copy(tolerance = tolerance))

  /**
   * Configures the request deadline rejected `percentage` (the maximum percentage
   * of requests that can be rejected due to request's deadline) of this server
   * (default: 20%).
   *
   * @see [[https://twitter.github.io/finagle/guide/Servers.html#request-deadline]]
   */
  def deadlineMaxRejectedPercentage(percentage: Double): A =
    self.configured(self.params[DeadlineFilter.Param].copy(maxRejectPercentage = percentage))
}
