package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl._

/**
 * SslServerConfiguration represents the collection of parameters that an engine factory
 * should use to configure a TLS server [[Engine]].
 *
 * @param keyCredentials The credentials used by the server engine to verify itself to a
 * remote peer.
 *
 * @param clientAuth Determines whether mutual authentication is desired or required by this
 * server engine.
 *
 * @param trustCredentials The credentials used by the server engine to validate a remote
 * peer's credentials.
 *
 * @param cipherSuites The cipher suites which should be used by a particular server engine.
 *
 * @param protocols The protocols which should be enabled for use with a particular server engine.
 *
 * @param applicationProtocols The ALPN or NPN protocols which should be supported by a particular
 * server engine.
 *
 * @param forceJdk when true, forces use of the JDK SslProvider. Note that until
 * JDK 9, this does not work with H2. For this reason, it's false by default.
 */
case class SslServerConfiguration(
  keyCredentials: KeyCredentials = KeyCredentials.Unspecified,
  clientAuth: ClientAuth = ClientAuth.Unspecified,
  trustCredentials: TrustCredentials = TrustCredentials.Unspecified,
  cipherSuites: CipherSuites = CipherSuites.Unspecified,
  protocols: Protocols = Protocols.Unspecified,
  applicationProtocols: ApplicationProtocols = ApplicationProtocols.Unspecified,
  forceJdk: Boolean = false)
