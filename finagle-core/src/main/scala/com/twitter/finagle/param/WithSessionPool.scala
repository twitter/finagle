package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withSessionPool` (default pool) API entry point.
 *
 * @see [[SessionPoolingParams]]
 */
trait WithSessionPool[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the client's session pool.
   *
   * All session pool settings are applied to each host in the replica set. Put this way, these
   * settings are per-host as opposed to per-client.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#pooling]]
   */
  val withSessionPool: SessionPoolingParams[A] = new SessionPoolingParams(self)
}
