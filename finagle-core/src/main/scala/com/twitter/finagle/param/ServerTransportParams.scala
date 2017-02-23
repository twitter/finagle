package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.ssl.{ApplicationProtocols, CipherSuites, KeyCredentials}
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.transport.{TlsConfig, Transport}
import java.io.File
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
   * Enables the TLS/SSL support (connection encrypting) on this server. Only `certificatePath` and
   * `keyPath` are required to build up a TLS/SSL transport.
   *
   * @param certificatePath the path to the PEM encoded X.509 certificate chain
   *
   * @param keyPath the path to the corresponding PEM encoded PKCS#8 private key
   *
   * @param caCertificatePath the path to the optional PEM encoded CA certificates trusted by this
   *                          server
   *
   * @param ciphers the list of supported ciphers, delimited by `:`
   *
   * @param nextProtocols the comma-delimited list of protocols used to perform APN
   *                      (Application Protocol Negotiation)
   */
  def tls(
    certificatePath: String,
    keyPath: String,
    caCertificatePath: Option[String],
    ciphers: Option[String],
    nextProtocols: Option[String]
  ): A = {
    val keyCredentials = caCertificatePath match {
      case Some(caPath) => KeyCredentials.CertKeyAndChain(
        new File(certificatePath), new File(keyPath), new File(caPath))
      case None => KeyCredentials.CertAndKey(
        new File(certificatePath), new File(keyPath))
    }
    val cipherSuites = ciphers match {
      case Some(suites) => CipherSuites.fromString(suites)
      case None => CipherSuites.Unspecified
    }
    val applicationProtocols = nextProtocols match {
      case Some(protos) => ApplicationProtocols.fromString(protos)
      case None => ApplicationProtocols.Unspecified
    }
    val configuration = SslServerConfiguration(
      keyCredentials = keyCredentials,
      cipherSuites = cipherSuites,
      applicationProtocols = applicationProtocols)

    self
      .configured(Transport.ServerSsl(Some(configuration)))
      .configured(Transport.Tls(TlsConfig.ServerCertAndKey(
        certificatePath, keyPath, caCertificatePath, ciphers, nextProtocols
      )))
  }

  /**
   * Enables TLS/SSL support (connection encrypting) on this server.
   *
   * @note This configuration method is only used to configure Netty 4 transports.
   *
   * @note It's recommended to not use [[SSLContext]] directly, but rely on Finagle to pick
   *       the most efficient TLS/SSL implementation available on your platform.
   *
   * @param context the SSL context to use
   */
  def tls(context: SSLContext): A =
    self.configured(Transport.Tls(TlsConfig.ServerSslContext(context)))
}
