package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.ssl.{Ssl, Engine}
import com.twitter.finagle.transport.Transport
import com.twitter.util.Duration
import java.net.{InetSocketAddress, SocketAddress}
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
   * Configures the TCP connection `timeout` of this client (default: 1 second).
   *
   * The connection timeout is the maximum amount of time a transport is allowed
   * to spend establishing a TCP connection.
   */
  def connectTimeout(timeout: Duration): A =
    self.configured(Transporter.ConnectTimeout(timeout))

  /**
   * Enables the TLS/SSL support (connection encrypting) on this client.
   * Hostname verification will be provided against the given `hostname`.
   */
  def tls(hostname: String): A = {
    val socketAddressToEngine: SocketAddress => Engine = {
      case sa: InetSocketAddress => Ssl.client(hostname, sa.getPort)
      case _ => Ssl.client()
    }

    self
      .configured(Transport.TLSClientEngine(Some(socketAddressToEngine)))
      .configured(Transporter.TLSHostname(Some(hostname)))
  }

  /**
   * Enables the TLS/SSL support (connection encrypting) with no hostname validation
   * on this client. The TLS/SSL sessions are configured using the given `context`.
   */
  def tls(context: SSLContext): A = {
    val socketAddressToEngine: SocketAddress => Engine = {
      case sa: InetSocketAddress => Ssl.client(context, sa.getHostName, sa.getPort)
      case _ => Ssl.client(context)
    }

    self.configured(Transport.TLSClientEngine(Some(socketAddressToEngine)))
  }

  /**
   * Enables the TLS/SSL support (connection encrypting) with no hostname validation
   * on this client.
   */
  def tlsWithoutValidation: A = {
    val socketAddressToEngine: SocketAddress => Engine = {
      case sa: InetSocketAddress =>
        Ssl.clientWithoutCertificateValidation(sa.getHostName, sa.getPort)
      case _ =>
        Ssl.clientWithoutCertificateValidation()
    }

    self.configured(Transport.TLSClientEngine(Some(socketAddressToEngine)))
  }

  /**
   * Enables the SOCKS proxy on this client (default: global flags).
   *
   * @param socketAddress the socket address of the proxy server
   *
   * @param credentials the optional credentials for the proxy server
   */
  def socksProxy(
    socketAddress: SocketAddress,
    credentials: Option[Transporter.Credentials]
  ): A = self.configured(Transporter.SocksProxy(
    Some(socketAddress),
    credentials.map(c => (c.username, c.password))
  ))
}
