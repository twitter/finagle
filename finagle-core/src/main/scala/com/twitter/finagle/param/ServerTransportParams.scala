package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.server.Listener
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.transport.Transport
import com.twitter.util.StorageUnit

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
   * Enables the TLS/SSL support (connection encrypting) on this server.
   *
   * @param certificatePath the path to the PEM encoded certificate file
   *
   * @param keyPath the path to the corresponding PEM encoded key file
   *
   * @param caCertificatePath the path to the optional PEM encoded CA cert file
   *                          (JSSE: If caCertPath is set, it should contain the
   *                          certificate and will be used in place of `certificatePath`)
   *
   * @param ciphers the ciphers spec (OpenSSL)
   *
   * @param nextProtocols the next protocols specs
   */
  def tls(
    certificatePath: String,
    keyPath: String,
    caCertificatePath: Option[String],
    ciphers: Option[String],
    nextProtocols: Option[String]
  ): A = self.configured(Transport.TLSServerEngine(
    Some(() =>
      Ssl.server(
        certificatePath, keyPath, caCertificatePath.orNull, ciphers.orNull, nextProtocols.orNull
      )
    )
  ))
}
