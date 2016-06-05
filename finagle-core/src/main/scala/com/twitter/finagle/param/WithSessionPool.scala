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
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#pooling]]
   */
  val withSessionPool: SessionPoolingParams[A] = new SessionPoolingParams(self)
}
