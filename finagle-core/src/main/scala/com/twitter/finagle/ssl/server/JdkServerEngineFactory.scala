package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl.{Engine, SslConfigurations}

/**
 * This engine factory is a default JVM-based implementation, intended to provide
 * coverage for a wide array of configurations.
 */
object JdkServerEngineFactory extends SslServerEngineFactory {

  /**
   * Creates a new [[Engine]] based on an [[SslServerConfiguration]].
   *
   * @param config A collection of parameters which the engine factory
   * should consider when creating the TLS server [[Engine]].
   *
   * @note [[ApplicationProtocols]] other than Unspecified are not supported.
   */
  def apply(config: SslServerConfiguration): Engine = {
    SslConfigurations.checkApplicationProtocolsNotSupported(
      "JdkServerEngineFactory",
      config.applicationProtocols
    )

    val sslContext =
      SslConfigurations.initializeSslContext(config.keyCredentials, config.trustCredentials)
    val engine = SslServerEngineFactory.createEngine(sslContext)
    SslServerEngineFactory.configureEngine(engine, config)
    engine
  }
}
