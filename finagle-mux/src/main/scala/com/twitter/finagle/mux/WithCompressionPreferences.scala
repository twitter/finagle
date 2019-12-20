package com.twitter.finagle.mux

import com.twitter.finagle.Stack

/**
 * Provides the `withTransport` (client or server transport) API entry point.
 *
 * @see [[CompressionParams]]
 */
trait WithCompressionPreferences[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the client or server's compression.
   */
  val withCompressionPreferences: CompressionParams[A] = new CompressionParams(self)
}
