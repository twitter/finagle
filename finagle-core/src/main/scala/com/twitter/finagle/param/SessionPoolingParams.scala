package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.client.DefaultPool
import com.twitter.util.Duration

/**
 * A collection of methods for configuring the Pooling module of Finagle clients.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[https://twitter.github.io/finagle/guide/Clients.html#pooling]]
 */
class SessionPoolingParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Configures the total number of temporary (may be closed and reestablished) and persistent
   * (remain open during the lifetime of a given client/pool per-host sessions of this client's
   * pool (default: unbounded).
   *
   * @note The session pool will not have more active sessions than `sessionsPerHost`.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#pooling]]
   */
  def maxSize(sessionsPerHost: Int): A =
    self.configured(self.params[DefaultPool.Param].copy(high = sessionsPerHost))

  /**
   * Configures the number of per-host persistent (remain open during the
   * lifetime of a given client/pool) sessions of this client's pool (default: 0).
   *
   * @note The session pool will not be shrinked below `sessionsPerHost`.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#pooling]]
   */
  def minSize(sessionsPerHost: Int): A =
    self.configured(self.params[DefaultPool.Param].copy(low = sessionsPerHost))

  /**
   *
   * Configures the maximum number of per-host sessions requests that are queued
   * when the connections concurrency exceeds the maximum size of this pool
   * (default: unbounded).
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#pooling]]
   */
  def maxWaiters(maxWaitersPerHost: Int): A =
    self.configured(self.params[DefaultPool.Param].copy(maxWaiters = maxWaitersPerHost))

  /**
   * Configures the session pool TTL timeout, the maximum amount of time
   * a given _temporary_ per-host session is allowed to be cached in a pool (default: unbounded).
   *
   * The expiration task runs at most per min(TTL, 1s), thus the "real" TTL is a uniform
   * distribution in the range [ttl, min(ttl + 1s, ttl * 2)). Put this way, it is acceptable to lag
   * the session expiration for at most TTL or 1 second, whichever is shorter.
   *
   * @note TTL does not apply to permanent sessions (up to `minSize`).
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#pooling]]
   */
  def ttl(timeout: Duration): A =
    self.configured(self.params[DefaultPool.Param].copy(idleTime = timeout))
}
