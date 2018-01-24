package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl.{Engine, SslConfigurations}
import javax.net.ssl.SSLContext

/*
 * This class provides an ability to use an initialized supplied
 * `javax.net.ssl.SSLContext` as the basis for creating [[Engine Engines]].
 */
final class SslContextServerEngineFactory(sslContext: SSLContext) extends SslServerEngineFactory {

  /**
   * Creates a new [[Engine]] based on an [[SslServerConfiguration]]
   * using the supplied `javax.net.ssl.SSLContext`.
   *
   * @param config A collection of parameters which the engine factory should
   * consider when creating the TLS server [[Engine]].
   *
   * @note [[KeyCredentials]] other than Unspecified are not supported.
   * @note [[TrustCredentials]] other than Unspecified are not supported.
   * @note [[ApplicationProtocols]] other than Unspecified are not supported.
   */
  def apply(config: SslServerConfiguration): Engine = {
    SslConfigurations.checkKeyCredentialsNotSupported(
      "SslContextServerEngineFactory",
      config.keyCredentials
    )
    SslConfigurations.checkTrustCredentialsNotSupported(
      "SslContextServerEngineFactory",
      config.trustCredentials
    )
    SslConfigurations.checkApplicationProtocolsNotSupported(
      "SslContextServerEngineFactory",
      config.applicationProtocols
    )

    val engine = SslServerEngineFactory.createEngine(sslContext)
    SslServerEngineFactory.configureEngine(engine, config)
    engine
  }
}
