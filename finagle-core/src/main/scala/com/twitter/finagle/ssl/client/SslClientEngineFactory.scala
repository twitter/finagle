package com.twitter.finagle.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl._
import java.net.InetSocketAddress
import javax.net.ssl.SSLContext

/**
 * Instances of this class provide a method to create Finagle
 * [[Engine Engines]] for use with TLS.
 */
private[finagle] abstract class SslClientEngineFactory {

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
  def apply(address: Address, config: SslClientConfiguration): Engine
}

private[finagle] object SslClientEngineFactory {

  /**
   * Configure the supplied [[Engine Engine's]] client mode,
   * protocols and cipher suites.
   */
  def configureEngine(engine: Engine, config: SslClientConfiguration): Unit = {
    val sslEngine = engine.self

    // Use true here to ensure that this engine is seen as a client engine.
    // This matters for handshaking.
    sslEngine.setUseClientMode(true)

    SslConfigurations.configureProtocols(sslEngine, config.protocols)
    SslConfigurations.configureCipherSuites(sslEngine, config.cipherSuites)
  }

  /**
   * Use the supplied `javax.net.ssl.SSLContext` to create a new
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
   * or fall back to the hostname of the `java.net.InetSocketAddress`.
   */
  def getHostname(isa: InetSocketAddress, config: SslClientConfiguration): String =
    config.hostname match {
      case Some(host) => host
      case None => isa.getHostName
    }

}
