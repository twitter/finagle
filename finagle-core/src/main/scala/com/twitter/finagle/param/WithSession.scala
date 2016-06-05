package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withSession` API entry point.
 *
 * @see [[SessionParams]]
 */
trait WithSession[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the client's sessions.
   *
   * Session might be viewed as logical connection that wraps a physical connection
   * (i.e., [[com.twitter.finagle.transport.Transport transport]]) and controls its
   * lifecycle. Sessions are used in Finagle to maintain liveness, requests cancellation,
   * draining, and many more.
   *
   * The default setup for a Finagle client's sessions is to not put any
   * timeouts on it.
   */
  val withSession: SessionParams[A] = new SessionParams(self)
}
