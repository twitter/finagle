package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withTransport` (client's transport) API entry point.
 *
 * @see [[ClientTransportParams]]
 */
trait WithClientTransport[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the client's [[com.twitter.finagle.transport.Transport]].
   *
   * `Transport` is a Finagle abstraction over the network connection (i.e., a TCP connection).
   */
  val withTransport: ClientTransportParams[A] = new ClientTransportParams(self)
}
