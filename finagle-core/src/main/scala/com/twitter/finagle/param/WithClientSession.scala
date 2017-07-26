package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withSession` (client's session) API entry point.
 *
 * @see [[ClientSessionParams]]
 */
trait WithClientSession[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the client's session.
   */
  val withSession: ClientSessionParams[A] = new ClientSessionParams(self)
}
