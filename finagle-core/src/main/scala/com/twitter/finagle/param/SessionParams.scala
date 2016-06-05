package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.service.ExpiringService
import com.twitter.util.Duration

/**
 * A collection of methods for configuring sessions of the Finagle clients.
 *
 * Session might be viewed as logical connection that wraps a physical connection
 * (i.e., [[com.twitter.finagle.transport.Transport transport]]) and controls its
 * lifecycle. Sessions are used in Finagle to maintain liveness, requests cancellation,
 * draining, and many more.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[SessionPoolingParams]] for pooling related configuration
 */
class SessionParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Configures the session acquisition `timeout` of this client (default: unbounded).
   *
   * This timeout is applied to the acquisition of a service and includes
   * both queueing time (e.g. because we cannot create more connections due
   * to connections limit and there are outstanding requests) as well as physical
   * TCP connection time. Futures returned from `factory()` will always be satisfied
   * within this timeout.
   *
   * This timeout also includes resolving logical destinations, but the cost of
   * resolution is amortized.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration]]
   */
  def acquisitionTimeout(timeout: Duration): A =
    self.configured(TimeoutFactory.Param(timeout))

  /**
   * Configures the session max idle time `timeout` - the maximum amount of time
   * a given session is allowed to be idle before it is closed (default: unbounded).
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration]]
   */
  def maxIdleTime(timeout: Duration): A =
    self.configured(self.params[ExpiringService.Param].copy(idleTime = timeout))

  /**
   * Configures the session lifetime `timeout` - the maximum amount of time a given
   * service is allowed to live before it is closed (default: unbounded).
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration]]
   */
  def maxLifeTime(timeout: Duration): A =
    self.configured(self.params[ExpiringService.Param].copy(lifeTime = timeout))
}
