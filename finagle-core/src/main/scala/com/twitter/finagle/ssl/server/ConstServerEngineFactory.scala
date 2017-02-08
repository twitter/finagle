package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl.{Engine, SslConfigurations}

/**
 * This engine factory is a bridge for TLSServerEngine param configurations
 * which use a () => Engine interface. It should only be used for legacy
 * purposes.
 */
class ConstServerEngineFactory(
    newEngine: () => Engine)
  extends SslServerEngineFactory {

  /**
   * Creates a new [[Engine]] based on an [[SslServerConfiguration]].
   *
   * @param config A collection of parameters which the engine factory
   * should consider when creating the TLS server [[Engine]].
   *
   * @note [[KeyCredentials]] other than Unspecified are not supported.
   * @note [[TrustCredentials]] other than Unspecified are not supported.
   * @note [[ApplicationProtocols]] other than Unspecified are not supported.
   */
  def apply(config: SslServerConfiguration): Engine = {
    SslConfigurations.checkKeyCredentialsNotSupported(
      "ConstServerEngineFactory", config.keyCredentials)
    SslConfigurations.checkTrustCredentialsNotSupported(
      "ConstServerEngineFactory", config.trustCredentials)
    SslConfigurations.checkApplicationProtocolsNotSupported(
      "ConstServerEngineFactory", config.applicationProtocols)

    val engine = newEngine()
    SslServerEngineFactory.configureEngine(engine, config)
    engine
  }

}
