package com.twitter.finagle.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl._
import java.net.InetSocketAddress
import javax.net.ssl.{SSLContext, SSLEngine}

/**
 * Instances of this trait provide a method to create Finagle
 * [[Engine Engines]] for use with TLS.
 */
private[ssl] trait SslClientEngineFactory {

  /**
   * Creates a new [[Engine]] based on an [[Address]]
   * and a [[SslClientConfiguration]].
   *
   * @param address A physical address which potentially includes
   * metadata.
   *
   * @param config A collection of parameters which the engine factory
   * should consider when creating the TLS client [[Engine]].
   */
  def mkEngine(address: Address, config: SslClientConfiguration): Engine
}

private[ssl] object SslClientEngineFactory {

  private[this] def enableClientMode(sslEngine: SSLEngine): Unit = {
    // Use true here for the mode to ensure this is a client (true) engine
    // instead of a server (false) engine. This matters for handshaking.
    sslEngine.setUseClientMode(true)
  }

  private[this] def configureCipherSuites(
    sslEngine: SSLEngine,
    config: SslClientConfiguration
  ): Unit = {
    config.cipherSuites match {
      case CipherSuites.Unspecified => // Do Nothing
      case CipherSuites.Enabled(ciphers) =>
        sslEngine.setEnabledCipherSuites(ciphers.toArray)
    }
  }

  private[this] def configureProtocols(
    sslEngine: SSLEngine,
    config: SslClientConfiguration
  ): Unit = {
    config.protocols match {
      case Protocols.Unspecified => // Do Nothing
      case Protocols.Enabled(protocols) =>
        sslEngine.setEnabledProtocols(protocols.toArray)
    }
  }

  /**
   * Configure the supplied [[Engine Engine's]] client mode,
   * protocols and cipher suites.
   */
  def configureEngine(engine: Engine, config: SslClientConfiguration): Unit = {
    val sslEngine = engine.self
    enableClientMode(sslEngine)
    configureProtocols(sslEngine, config)
    configureCipherSuites(sslEngine, config)
  }

  /**
   * Use the supplied [[javax.net.ssl.SSLContext]] to create a new
   * [[Engine]] based on a hostname and port, if available.
   */
  def createEngine(
    sslContext: SSLContext,
    address: Address,
    config: SslClientConfiguration
  ): Engine = {
    val sslEngine = address match {
      case Address.Inet(isa, _) => sslContext.createSSLEngine(getHostname(isa, config), isa.getPort)
      case _ => sslContext.createSSLEngine()
    }
    new Engine(sslEngine)
  }

  /**
   * Return the hostname from the [[SslClientConfiguration configuration]] if set,
   * or fall back to the hostname of the [[java.net.InetSocketAddress]].
   */
  def getHostname(isa: InetSocketAddress, config: SslClientConfiguration): String =
    config.hostname match {
      case Some(host) => host
      case None => isa.getHostName
    }

  /**
   * Guard method for failing fast inside of a factory's mkEngine method when
   * [[KeyCredentials]] are not supported.
   */
  def checkKeyCredentialsNotSupported(
    engineFactoryName: String,
    config: SslClientConfiguration
  ): Unit = {
    config.keyCredentials match {
      case KeyCredentials.Unspecified => // Do Nothing
      case KeyCredentials.CertAndKey(_, _) =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertAndKey", engineFactoryName)
      case KeyCredentials.CertKeyAndChain(_, _, _) =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertKeyAndChain", engineFactoryName)
    }
  }

  /**
   * Guard method for failing fast inside of a factory's mkEngine method when
   * [[TrustCredentials]] are not supported.
   */
  def checkTrustCredentialsNotSupported(
    engineFactoryName: String,
    config: SslClientConfiguration
  ): Unit = {
    config.trustCredentials match {
      case TrustCredentials.Unspecified => // Do Nothing
      case TrustCredentials.Insecure =>
        throw SslConfigurationException.notSupported(
          "TrustCredentials.Insecure", engineFactoryName)
      case TrustCredentials.CertCollection(_) =>
        throw SslConfigurationException.notSupported(
          "TrustCredentials.CertCollection", engineFactoryName)
    }
  }

  /**
   * Guard method for failing fast inside of a factory's mkEngine method when
   * [[ApplicationProtocols]] are not supported.
   */
  def checkApplicationProtocolsNotSupported(
    engineFactoryName: String,
    config: SslClientConfiguration
  ): Unit = {
    config.applicationProtocols match {
      case ApplicationProtocols.Unspecified => // Do Nothing
      case ApplicationProtocols.Supported(_) =>
        throw SslConfigurationException.notSupported(
          "ApplicationProtocols.Supported", engineFactoryName)
    }
  }

}
