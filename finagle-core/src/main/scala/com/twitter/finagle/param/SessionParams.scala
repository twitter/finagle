package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.service.ExpiringService
import com.twitter.util.Duration

/**
 * A collection of methods for configuring sessions of Finagle clients and servers.
 *
 * Session might be viewed as logical connection that wraps a physical connection
 * (i.e., [[com.twitter.finagle.transport.Transport transport]]) and controls its
 * lifecycle. Sessions are used in Finagle to maintain liveness, requests cancellation,
 * draining, and much more.
 *
 * @tparam A a [[Stack.Parameterized]] client or server to configure
 */
class SessionParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Configures the session lifetime `timeout` - the maximum amount of time a given
   * connection is allowed to live before it is closed (default: unbounded).
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration]]
   */
  def maxLifeTime(timeout: Duration): A =
    self.configured(self.params[ExpiringService.Param].copy(lifeTime = timeout))
}
