package com.twitter.finagle.transport

import javax.net.ssl.SSLContext

/**
 * An ADT representing a [[https://en.wikipedia.org/wiki/Transport_Layer_Security TLS]] config used
 * by the underlying transport implementation.
 */
sealed trait TlsConfig

object TlsConfig {

  /**
   * Indicates that TLS is disabled on a given [[Transport]].
   */
  case object Disabled extends TlsConfig

  /**
   * Server-side TLS config based on certificate and key.
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
  final case class ServerCertAndKey(
      certificatePath: String,
      keyPath: String,
      caCertificatePath: Option[String],
      ciphers: Option[String],
      nextProtocols: Option[String]) extends TlsConfig

  /**
   * Server-side TLS config based on [[SSLContext]].
   */
  final case class ServerSslContext(context: SSLContext) extends TlsConfig

  /**
   * Client-side TLS config that requires hostname verification against the given `hostname`.
   */
  final case class ClientHostname(hostname: String) extends TlsConfig

  /**
   * Client-side TLS config based on a given [[SSLContext]].
   */
  final case class ClientSslContext(content: SSLContext) extends TlsConfig

  /**
   * Client-side TLS config based on a given [[SSLContext]] that also requires hostname verification
   * against the given `hostname`.
   */
  final case class ClientSslContextAndHostname(content: SSLContext, hostname: String) extends TlsConfig

  /**
   * Client-side TLS config that doesn't require certificate validation.
   *
   * @note That's probably a bad idea to use this anywhere, but testing/development.
   */
  case object ClientNoValidation extends TlsConfig

  /**
   * Client-side TLS config built out of default settings.
   */
  case object Client extends TlsConfig
}
