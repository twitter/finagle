package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withTransport` (server's transport) API entry point.
 *
 * @see [[ServerTransportParams]]
 */
trait WithServerTransport[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring servers' [[com.twitter.finagle.transport.Transport]].
   *
   * `Transport` is a Finagle abstraction over the network connection (i.e., a TCP connection).
   */
  val withTransport: ServerTransportParams[A] = {
    new ServerTransportParams(self)
  }

}
