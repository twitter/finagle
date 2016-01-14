package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Duration, StorageUnit}

/**
 * A collection of methods for configuring the [[Transport]] params of
 * Finagle servers or clients.
 *
 * [[Transport]] is a Finagle abstraction over the network connection
 * (i.e., a TCP connection).
 *
 * @note This class is abstract in a sense that it's base for both
 *       server and client transports.
 *
 * @tparam A a [[Stack.Parameterized]] server/client to configure
 *
 * @see [[ClientTransportParams]] for client-specific params
 *      [[ServerTransportParams]] for server-specific params
 */
abstract class TransportParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Configures this client or server with given TCP send buffer `size` (default: unspecified,
   * a system-level value will be used).
   */
  def sendBufferSize(size: StorageUnit): A =
    self.configured(self.params[Transport.BufferSizes].copy(send = Some(size.inBytes.toInt)))

  /**
   * Configures this client or server with given TCP receive buffer `size` (default: unspecified,
   * a system-level value will be used).
   */
  def receiveBufferSize(size: StorageUnit): A =
    self.configured(self.params[Transport.BufferSizes].copy(recv = Some(size.inBytes.toInt)))

  /**
   * Configures this client or server with given transport-level read `timeout`
   * (default: unbounded).
   *
   * The transport-level read timeout is the maximum amount of time a
   * connection/transport may have received no data.
   */
  def readTimeout(timeout: Duration): A =
    self.configured(self.params[Transport.Liveness].copy(readTimeout = timeout))

  /**
   * Configures this client or server with given transport-level write `timeout`
   * (default: unbounded).
   *
   * The transport-level write timeout is the maximum amount of time a
   * connection/transport may not have sent any data.
   */
  def writeTimeout(timeout: Duration): A =
    self.configured(self.params[Transport.Liveness].copy(writeTimeout = timeout))

  /**
   * Makes the transport of this client or server verbose (default: disabled).
   *
   * A verbose transport logs its activity onto a configured logger (by default,
   * standard output).
   */
  def verbose: A =
    self.configured(Transport.Verbose(enabled = true))
}
