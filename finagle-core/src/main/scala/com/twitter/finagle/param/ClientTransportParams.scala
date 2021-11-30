package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.client.SslClientEngineFactory
import com.twitter.finagle.ssl.client.SslClientSessionVerifier
import com.twitter.finagle.ssl.client.SslContextClientEngineFactory
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.Transport.ClientSsl
import com.twitter.util.Duration
import javax.net.ssl.SSLContext

/**
 * A collection of methods for configuring the [[Transport]] for Finagle clients.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[com.twitter.finagle.param.TransportParams]]
 */
class ClientTransportParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A])
    extends TransportParams(self) {

  /**
   * Configures the socket connect `timeout` of this client (default: 1 second).
   *
   * The connection timeout is the maximum amount of time a transport is allowed
   * to spend connecting to a remote socket. This does not include an actual session creation
   * (SSL handshake, HTTP proxy handshake, etc.) only raw socket connect.
   */
  def connectTimeout(timeout: Duration): A =
    self.configured(Transporter.ConnectTimeout(timeout))

  /**
   * Enables SSL/TLS support (connection encrypting) on this client.
   */
  def tls(config: SslClientConfiguration): A =
    self.configured(Transport.ClientSsl(Some(config)))

  /**
   * Enables SSL/TLS support (connection encrypting) on this client.
   */
  def tls(config: SslClientConfiguration, engineFactory: SslClientEngineFactory): A =
    self
      .configured(Transport.ClientSsl(Some(config)))
      .configured(SslClientEngineFactory.Param(engineFactory))

  /**
   * Enables SSL/TLS support (connection encrypting) on this client.
   */
  def tls(config: SslClientConfiguration, sessionVerifier: SslClientSessionVerifier): A =
    self
      .configured(Transport.ClientSsl(Some(config)))
      .configured(SslClientSessionVerifier.Param(sessionVerifier))

  /**
   * Enables SSL/TLS support (connection encrypting) on this client.
   */
  def tls(
    config: SslClientConfiguration,
    engineFactory: SslClientEngineFactory,
    sessionVerifier: SslClientSessionVerifier
  ): A =
    self
      .configured(Transport.ClientSsl(Some(config)))
      .configured(SslClientEngineFactory.Param(engineFactory))
      .configured(SslClientSessionVerifier.Param(sessionVerifier))

  /**
   * Enables SSL/TLS support (connection encrypting) on this client.
   *
   * @note Given that this uses default [[SSLContext]], all configuration params (trust/key stores)
   *       should be passed as Java system properties.
   */
  def tls: A =
    self
      .configured(Transport.ClientSsl(Some(SslClientConfiguration())))

  /**
   * Enables SSL/TLS support (connection encrypting) on this client.
   * Hostname verification will be provided against the given `hostname`.
   */
  def tls(hostname: String): A =
    self
      .configured(Transport.ClientSsl(Some(SslClientConfiguration(hostname = Some(hostname)))))

  /**
   * Enables SSL/TLS support (connection encrypting) with no hostname validation
   * on this client. The SSL/TLS are configured using the given `context`.
   *
   * @note It's recommended to not use [[SSLContext]] directly, but rely on Finagle to pick
   *       the most efficient SSL/TLS available on your platform.
   */
  def tls(context: SSLContext): A =
    self
      .configured(SslClientEngineFactory.Param(new SslContextClientEngineFactory(context)))
      .configured(Transport.ClientSsl(Some(SslClientConfiguration())))

  /**
   * Enables the TLS/SSL support (connection encrypting) with hostname validation
   * on this client. The TLS/SSL sessions are configured using the given `context`.
   */
  def tls(context: SSLContext, hostname: String): A =
    self
      .configured(SslClientEngineFactory.Param(new SslContextClientEngineFactory(context)))
      .configured(Transport.ClientSsl(Some(SslClientConfiguration(hostname = Some(hostname)))))

  /**
   * Enables the TLS/SSL support (connection encrypting) with no certificate validation
   * on this client.
   *
   * @note This makes a client trust any certificate sent by a server, which invalidates the entire
   *       idea of TLS/SSL. Use this carefully.
   */
  def tlsWithoutValidation: A = {
    self
      .configured(
        Transport
          .ClientSsl(Some(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)))
      )
  }

  /**
   * Configures SNI hostname for SSL
   *
   * @see [[https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SNIHostName.html Java's
   * SNIHostName]] for more details.
   */

  def sni(hostname: String): A = {
    val config = self.params[ClientSsl].sslClientConfiguration match {
      case Some(config) => config.copy(sniHostName = Some(hostname))
      case None => SslClientConfiguration(sniHostName = Some(hostname))
    }
    self.configured(Transport.ClientSsl(Some(config)))
  }

  /**
   * Enables TCP tunneling via `HTTP CONNECT` through an HTTP proxy [1] on this client
   * (default: disabled).
   *
   * TCP tunneling might be used to flow any TCP traffic (not only HTTP), but is mostly used to
   * establish an HTTPS (TLS/SSL over HTTP) connection to a remote HTTP server through a proxy.
   *
   * When enabled, a Finagle client treats the server it connects to as a proxy server and asks it
   * to proxy the traffic to a given ultimate destination, specified as `host`.
   *
   * [1]: https://tools.ietf.org/html/draft-luotonen-web-proxy-tunneling-01
   *
   * @param host the ultimate host a proxy server connects to
   */
  def httpProxyTo(host: String): A =
    self.configured(Transporter.HttpProxyTo(Some(host -> None)))

  /**
   * Enables TCP tunneling via `HTTP CONNECT` through an HTTP proxy [1] on this client
   * (default: disabled).
   *
   * TCP tunneling might be used to flow any TCP traffic (not only HTTP), but is mostly used to
   * establish an HTTPS (TLS/SSL over HTTP) connection to a remote HTTP server through a proxy.
   *
   * When enabled, a Finagle client treats the server it connects to as a proxy server and asks it
   * to proxy the traffic to a given ultimate destination, specified as `host`.
   *
   * [1]: https://tools.ietf.org/html/draft-luotonen-web-proxy-tunneling-01
   *
   * @param host the ultimate host a proxy server connects to
   *
   * @param credentials credentials for a proxy server
   */
  def httpProxyTo(host: String, credentials: Transporter.Credentials): A =
    self.configured(Transporter.HttpProxyTo(Some(host -> Some(credentials))))
}
