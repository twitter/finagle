package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.ssl.{
  ApplicationProtocols,
  Engine,
  SslConfigurationException,
  TrustCredentials
}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory}
import io.netty.handler.ssl.{OpenSsl, SslContext}

/**
 * This engine factory is intended to be used *only* by clients that talk to
 * outside web servers. It ignores some of the parameters passed in as part
 * of the `SslClientConfiguration`, in order to severely limit the number
 * of `OpenSslContext` objects which get created.
 *
 * There are four types of engines which can be created by this engine factory:
 * 1. http/1.1 with trust credential checking done against system certs
 * 2. http/1.1 with no trust credential checking done (insecure)
 * 3. h2 with trust credential checking done against system certs
 * 4. h2 with no trust credential checking done (insecure)
 *
 * @note `KeyCredentials`, `CipherSuites`, and `Protocols` values are all
 * ignored with this engine factory and are treated as if they were left
 * as `Unspecified`.
 */
object ExternalClientEngineFactory extends SslClientEngineFactory {

  // use the default allocator
  private val allocator = Allocator.allocatorParam.default.allocator
  private val forceJdk = !OpenSsl.isAvailable
  private val http2AppProtocols = ApplicationProtocols.Supported(Seq("h2", "http/1.1"))

  // Create the configs to use for the contexts
  private val withValidationConfig = SslClientConfiguration()
  private val withoutValidationConfig = SslClientConfiguration(
    trustCredentials = TrustCredentials.Insecure
  )

  private val withValidation2Config =
    withValidationConfig.copy(applicationProtocols = http2AppProtocols)
  private val withoutValidation2Config =
    withoutValidationConfig.copy(applicationProtocols = http2AppProtocols)

  // Create the contexts using the configs
  private val withValidationContext: SslContext =
    Netty4ClientSslConfigurations.createClientContext(withValidationConfig, forceJdk)
  private val withoutValidationContext: SslContext =
    Netty4ClientSslConfigurations.createClientContext(withoutValidationConfig, forceJdk)

  // HTTP 2 will not work (until JDK 9) without the native engine, so set forceJdk to false
  private val withValidation2Context: SslContext =
    Netty4ClientSslConfigurations.createClientContext(withValidation2Config, false)
  private val withoutValidation2Context: SslContext =
    Netty4ClientSslConfigurations.createClientContext(withoutValidation2Config, false)

  private def usingHttp2(config: SslClientConfiguration): Boolean =
    config.applicationProtocols match {
      case ApplicationProtocols.Supported(supported) => supported.contains("h2")
      case _ => false
    }

  private def selectContext(config: SslClientConfiguration): SslContext =
    config.trustCredentials match {
      case TrustCredentials.Unspecified =>
        if (usingHttp2(config)) withValidation2Context
        else withValidationContext
      case TrustCredentials.Insecure =>
        if (usingHttp2(config)) withoutValidation2Context
        else withoutValidationContext
      case _ =>
        throw SslConfigurationException.notSupported(
          "Unknown TrustCredentials value",
          "ExternalClientEngineFactory"
        )
    }

  def apply(address: Address, config: SslClientConfiguration): Engine = {
    val context = selectContext(config)
    Netty4ClientSslConfigurations.createClientEngine(address, config, context, allocator)
  }

}
