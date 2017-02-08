package com.twitter.finagle.ssl

/**
 * ClientAuth represents whether one-way or two-way TLS should
 * be used with a TLS [[Engine]]. One-way TLS authentication is
 * where the server sends its certificate to the client for
 * verification. Two-way or mutual TLS is where the server
 * sends its certificate to the client and the client sends its
 * certificate to the server for verification.
 *
 * This parameter is only used by [[SslServerConfiguration]].
 *
 * @note Not all engine factories provide support for client
 * authentication, especially with some underlying native engines.
 *
 * @note Java users: See [[ClientAuthConfig]].
 */
sealed trait ClientAuth

object ClientAuth {

  /**
   * Indicates that the determination for whether to use client
   * authentication is delegated to the engine factory.
   */
  case object Unspecified extends ClientAuth

  /**
   * Indicates that this server does not desire client
   * authentication with this TLS [[Engine]].
   */
  case object Off extends ClientAuth

  /**
   * Indicates that this server desires client authentication,
   * but that it is not required.
   */
  case object Wanted extends ClientAuth

  /**
   * Indicates that this server requires client authentication.
   */
  case object Needed extends ClientAuth
}
