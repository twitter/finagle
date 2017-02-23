package com.twitter.finagle.ssl.server

import com.twitter.finagle.ssl._

/**
 * This engine factory is intended to act as a temporary bridge for those currently using
 * [[JSSE]] or [[OpenSSL]] (Finagle-Native) through the [[Ssl]] class. From there, if using
 * [[JSSE]], then [[JdkServerEngineFactory]] or `Netty4ServerEngineFactory` should be the next
 * step. If using [[OpenSSL]], then `Netty4ServerEngineFactory` should be the next
 * step, as it will be the supported path forward for using native engines with Finagle.
 */
@deprecated("Use Netty4ServerEngineFactory instead", "2017-02-07")
object LegacyServerEngineFactory extends SslServerEngineFactory {

  private[this] def cipherSuitesToString(cipherSuites: CipherSuites): Option[String] = {
    cipherSuites match {
      case CipherSuites.Unspecified => None
      case CipherSuites.Enabled(ciphers) =>
        if (ciphers.isEmpty) None else Some(ciphers.mkString(":"))
    }
  }

  private[this] def applicationProtocolsToString(
    applicationProtocols: ApplicationProtocols
  ): Option[String] = {
    applicationProtocols match {
      case ApplicationProtocols.Unspecified => None
      case ApplicationProtocols.Supported(appProtos) =>
        if (appProtos.isEmpty) None else Some(appProtos.mkString(","))
    }
  }

  /**
   * Creates a new [[Engine]] based on an [[SslServerConfiguration]].
   *
   * @param config A collection of parameters which the engine factory
   * should consider when creating the TLS server [[Engine]].
   *
   * @note [[TrustCredentials]] other than Unspecified are not supported.
   * @note [[Protocols]] other than Unspecified are not supported.
   * @note [[ClientAuth]] values other than Unspecified are not supported.
   *
   * @note [[CipherSuites]] other than Unspecified are not supported if
   * the `OpenSSL` engine cannot be used and it falls back to the `JSSE`
   * engine.
   *
   * @note [[ApplicationProtocols]] other than Unspecified are not supported
   * if the `OpenSSL` engine cannot be used and it falls back to the `JSSE`
   * engine.
   */
  def apply(config: SslServerConfiguration): Engine = {
    SslConfigurations.checkTrustCredentialsNotSupported(
      "LegacyServerEngineFactory", config.trustCredentials)
    SslConfigurations.checkProtocolsNotSupported(
      "LegacyServerEngineFactory", config.protocols)
    SslConfigurations.checkClientAuthNotSupported(
      "LegacyServerEngineFactory", config.clientAuth)

    val cipherSuites: Option[String] = cipherSuitesToString(config.cipherSuites)
    val appProtos: Option[String] = applicationProtocolsToString(config.applicationProtocols)

    val engine = config.keyCredentials match {
      case KeyCredentials.Unspecified =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.Unspecified", "LegacyServerEngineFactory")
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        Ssl.server(certFile.getAbsolutePath(), keyFile.getAbsolutePath(),
          null, cipherSuites.orNull, appProtos.orNull)
      case KeyCredentials.CertKeyAndChain(certFile, keyFile, chainFile) =>
        Ssl.server(certFile.getAbsolutePath(), keyFile.getAbsolutePath(),
          chainFile.getAbsolutePath(), cipherSuites.orNull, appProtos.orNull)
    }

    // Explicitly set this to server mode, since calls to Ssl.server do not.
    engine.self.setUseClientMode(false)
    engine
  }

}
