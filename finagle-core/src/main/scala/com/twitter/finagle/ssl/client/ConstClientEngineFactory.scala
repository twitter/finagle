package com.twitter.finagle.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.{Engine, SslConfigurations}
import java.net.{InetSocketAddress, SocketAddress}

/**
 * This class represents a Finagle [[Address]] that doesn't have
 * or contain a corresponding `java.net.SocketAddress`
 */
case object UnknownSocketAddress extends SocketAddress

/**
 * This engine factory is a bridge for TLSClientEngine param configurations
 * which use a SocketAddress => Engine interface. It should only be used
 * for legacy purposes.
 */
class ConstClientEngineFactory(
    newEngine: SocketAddress => Engine)
  extends SslClientEngineFactory {

  private[this] def addressToSocketAddress(
    address: Address,
    config: SslClientConfiguration
  ): SocketAddress =
    address match {
      case Address.Inet(isa, _) =>
        // An SslClientConfiguration may specify a hostname, and if so, we need to
        // use that instead. It is why we create a new `InetSocketAddress` instead
        // of just returning the one from Address.Inet.
        new InetSocketAddress(SslClientEngineFactory.getHostname(isa, config), isa.getPort)
      case _ => UnknownSocketAddress
    }

  /**
   * Creates a new [[Engine]] based on an [[Address]] and an [[SslClientConfiguration]].
   *
   * @param address A physical address which potentially includes metadata.
   *
   * @param config A collection of parameters which the engine factory should
   * consider when creating the TLS client [[Engine]].
   *
   * @note [[KeyCredentials]] other than Unspecified are not supported.
   * @note [[TrustCredentials]] other than Unspecified are not supported.
   * @note [[ApplicationProtocols]] other than Unspecified are not supported.
   */
  def apply(address: Address, config: SslClientConfiguration): Engine = {
    SslConfigurations.checkKeyCredentialsNotSupported(
      "ConstClientEngineFactory", config.keyCredentials)
    SslConfigurations.checkTrustCredentialsNotSupported(
      "ConstClientEngineFactory", config.trustCredentials)
    SslConfigurations.checkApplicationProtocolsNotSupported(
      "ConstClientEngineFactory", config.applicationProtocols)

    val engine = newEngine(addressToSocketAddress(address, config))
    SslClientEngineFactory.configureEngine(engine, config)
    engine
  }

}
