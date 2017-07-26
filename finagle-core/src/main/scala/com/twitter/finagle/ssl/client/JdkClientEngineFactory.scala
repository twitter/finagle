package com.twitter.finagle.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.{Engine, SslConfigurations}

/**
 * This engine factory is a default JVM-based implementation, intended to provide
 * coverage for a wide array of configurations.
 */
object JdkClientEngineFactory extends SslClientEngineFactory {

  /**
   * Creates a new [[Engine]] based on an [[Address]] and an [[SslClientConfiguration]].
   *
   * @param address A physical address which potentially includes metadata.
   *
   * @param config A collection of parameters which the engine factory should
   * consider when creating the TLS client [[Engine]].
   *
   * @note [[ApplicationProtocols]] other than Unspecified are not supported.
   */
  def apply(address: Address, config: SslClientConfiguration): Engine = {
    SslConfigurations.checkApplicationProtocolsNotSupported(
      "JdkClientEngineFactory",
      config.applicationProtocols
    )

    val sslContext =
      SslConfigurations.initializeSslContext(config.keyCredentials, config.trustCredentials)
    val engine = SslClientEngineFactory.createEngine(sslContext, address, config)
    SslClientEngineFactory.configureEngine(engine, config)
    engine
  }

}
