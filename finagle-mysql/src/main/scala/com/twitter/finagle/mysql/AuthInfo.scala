package com.twitter.finagle.mysql

import com.twitter.finagle.stats.Counter

/**
 * Information needed at every step of authenticating with the
 * `caching_sha2_password` method.
 *
 * @param serverVersion the version of the server being connected to
 * @param settings the settings for the handshake, this includes the user's credentials
 * @param fastAuthSuccessCounter a counter used during testing to verify fast authentication
 *                               has successfully happened
 * @param tlsEnabled true if tls is enabled for this connection
 * @param salt the salt sent from the server for authentication
 */
private[mysql] case class AuthInfo(
  serverVersion: String,
  settings: HandshakeSettings,
  fastAuthSuccessCounter: Counter,
  tlsEnabled: Boolean = false,
  salt: Option[Array[Byte]] = None)
