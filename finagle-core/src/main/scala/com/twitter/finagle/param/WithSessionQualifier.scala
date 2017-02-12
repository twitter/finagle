package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withSessionQualifier` API entry point.
 *
 * @see [[SessionQualificationParams]]
 */
trait WithSessionQualifier[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the client's session qualifiers
   * (e.g. circuit breakers).
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#circuit-breaking]]
   */
  val withSessionQualifier: SessionQualificationParams[A] = new SessionQualificationParams(self)
}
