package com.twitter.finagle.param

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.Stack
import com.twitter.finagle.filter.RequestSemaphoreFilter
import com.twitter.finagle.service.DeadlineFilter

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
   *  Configures mode for `DeadlineFilter` to `Enabled`. (default: `Disabled`)
   */
  def deadlines: A = {
    self.configured(DeadlineFilter.Mode(DeadlineFilter.Mode.Enabled))
  }

  /**
   *  Configures mode for `DeadlineFilter` to `DarkMode`. (default: `Disabled`)
   */
  def darkModeDeadlines: A = {
    self.configured(DeadlineFilter.Mode(DeadlineFilter.Mode.DarkMode))
  }

  /**
   *  Configures mode for `DeadlineFilter` to `Disabled`. (default: `Disabled`)
   */
  def noDeadlines: A = {
    self.configured(DeadlineFilter.Mode(DeadlineFilter.Mode.Disabled))
  }
}
