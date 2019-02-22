package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.ssl.server.{
  SslContextServerEngineFactory,
  SslServerConfiguration,
  SslServerEngineFactory,
  SslServerSessionVerifier
}
import com.twitter.finagle.transport.Transport
import javax.net.ssl.SSLContext

/**
 * A collection of methods for configuring the [[Transport]] for Finagle servers.
 *
 * @tparam A a [[Stack.Parameterized]] server to configure
 *
 * @see [[com.twitter.finagle.param.TransportParams]]
 */
class ServerTransportParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A])
    extends TransportParams(self) {

  /**
   * Enables SSL/TLS support (connection encrypting) on this server.
   */
  def tls(config: SslServerConfiguration): A =
    self.configured(Transport.ServerSsl(Some(config)))

  /**
   * Enables SSL/TLS support (connection encrypting) on this server.
   */
  def tls(config: SslServerConfiguration, engineFactory: SslServerEngineFactory): A =
    self
      .configured(Transport.ServerSsl(Some(config)))
      .configured(SslServerEngineFactory.Param(engineFactory))

  /**
   * Enables SSL/TLS support (connection encrypting) on this server.
   */
  def tls(config: SslServerConfiguration, sessionVerifier: SslServerSessionVerifier): A =
    self
      .configured(Transport.ServerSsl(Some(config)))
      .configured(SslServerSessionVerifier.Param(sessionVerifier))

  /**
   * Enables SSL/TLS support (connection encrypting) on this server.
   */
  def tls(
    config: SslServerConfiguration,
    engineFactory: SslServerEngineFactory,
    sessionVerifier: SslServerSessionVerifier
  ): A =
    self
      .configured(Transport.ServerSsl(Some(config)))
      .configured(SslServerEngineFactory.Param(engineFactory))
      .configured(SslServerSessionVerifier.Param(sessionVerifier))

  /**
   * Enables TLS/SSL support (connection encrypting) on this server.
   *
   * @note It's recommended to not use [[SSLContext]] directly, but rely on Finagle to pick
   *       the most efficient TLS/SSL implementation available on your platform.
   *
   * @param context the SSL context to use
   */
  def tls(context: SSLContext): A =
    tls(SslServerConfiguration(), new SslContextServerEngineFactory(context))

}
