package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.netty4.ssl.FinalizedSslContext
import com.twitter.finagle.netty4.ssl.Netty4SslConfigurations
import com.twitter.finagle.ssl.CipherSuites
import com.twitter.finagle.ssl.ApplicationProtocols
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.KeyCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.client.SslClientEngineFactory
import com.twitter.util.Return
import com.twitter.util.security.PrivateKeyFile
import com.twitter.util.security.X509CertificateFile
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.SslContext
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol
import scala.collection.JavaConverters._

/**
 * Convenience functions for setting values on a Netty `SslContextBuilder`
 * which are applicable to only client configurations and engines.
 */
private[finagle] object Netty4ClientSslConfigurations {

  /**
   * Configures the application protocols of the `SslContextBuilder`. This
   * method mutates the `SslContextBuilder`, and returns it as the result.
   *
   * @note This sets which application level protocol negotiation to
   * use ALPN.
   *
   * @note This also sets the `SelectorFailureBehavior` to NO_ADVERTISE,
   * and the `SelectedListenerFailureBehavior` to ACCEPT as those are the
   * only modes supported by both JDK and Native engines.
   */
  private def configureClientApplicationProtocols(
    builder: SslContextBuilder,
    applicationProtocols: ApplicationProtocols
  ): SslContextBuilder = {
    // don't use NPN because https://github.com/netty/netty/issues/7346 breaks
    // web crawlers
    Netty4SslConfigurations.configureApplicationProtocols(
      builder,
      applicationProtocols,
      Protocol.ALPN
    )
  }

  /**
   * Creates an `SslContextBuilder` for a client with the supplied `KeyCredentials`.
   *
   * @note An `SslConfigurationException` will be thrown if there is an issue loading
   * the certificate(s) or private key.
   *
   * @note Will not validate the validity for certificates when configured
   *       with [[KeyCredentials.KeyManagerFactory]] in contrast to when
   *       configured with [[KeyCredentials.CertAndKey]], [[KeyCredentials.CertsAndKey]],
   *       or [[KeyCredentials.CertKeyAndChain]].
   */
  private def startClientWithKey(keyCredentials: KeyCredentials): SslContextBuilder = {
    val builder: SslContextBuilder = SslContextBuilder.forClient()
    val withKey = keyCredentials match {
      case KeyCredentials.Unspecified =>
        Return(builder) // Do Nothing
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        for {
          key <- new PrivateKeyFile(keyFile).readPrivateKey()
          cert <- new X509CertificateFile(certFile).readX509Certificate()
        } yield builder.keyManager(key, cert)
      case KeyCredentials.CertsAndKey(certsFile, keyFile) =>
        for {
          key <- new PrivateKeyFile(keyFile).readPrivateKey()
          certs <- new X509CertificateFile(certsFile).readX509Certificates()
        } yield builder.keyManager(key, certs: _*)
      case KeyCredentials.CertKeyAndChain(certFile, keyFile, chainFile) =>
        for {
          key <- new PrivateKeyFile(keyFile).readPrivateKey()
          cert <- new X509CertificateFile(certFile).readX509Certificate()
          chain <- new X509CertificateFile(chainFile).readX509Certificates()
        } yield builder.keyManager(key, cert +: chain: _*)
      case KeyCredentials.KeyManagerFactory(keyManagerFactory) =>
        Return(builder.keyManager(keyManagerFactory))
    }
    Netty4SslConfigurations.unwrapTryContextBuilder(withKey)
  }

  /**
   * Creates an `SslContext` based on the supplied `SslClientConfiguration`. This method uses
   * the `KeyCredentials`, `TrustCredentials`, `CipherSuites`, and `ApplicationProtocols` from the provided
   * configuration, and forces the JDK provider if forceJdk is true.
   */
  def createClientContext(config: SslClientConfiguration, forceJdk: Boolean): SslContext = {
    val builder = startClientWithKey(config.keyCredentials)
    val withProvider = Netty4SslConfigurations.configureProvider(builder, forceJdk)
    val withTrust = Netty4SslConfigurations.configureTrust(withProvider, config.trustCredentials)
    val withCiphers = config.cipherSuites match {
      case CipherSuites.Enabled(s) => withTrust.ciphers(s.asJava)
      case _ => withTrust
    }
    val withAppProtocols =
      configureClientApplicationProtocols(withCiphers, config.applicationProtocols)

    // We only want to use the `FinalizedSslContext` if we're using the non-JDK implementation.
    if (!forceJdk) new FinalizedSslContext(withAppProtocols.build())
    else withAppProtocols.build()
  }

  /**
   * Creates an `Engine` based on the supplied `Address`, `SslContext`, and `ByteBufAllocator`, and
   * then configures the underlying `SSLEngine` based on the supplied `SslClientConfiguration`.
   */
  def createClientEngine(
    address: Address,
    config: SslClientConfiguration,
    context: SslContext,
    allocator: ByteBufAllocator
  ): Engine = {
    val sslEngine = address match {
      case Address.Inet(isa, _) =>
        context.newEngine(allocator, SslClientEngineFactory.getHostString(isa, config), isa.getPort)
      case _ =>
        context.newEngine(allocator)
    }
    val engine = Engine(sslEngine)
    SslClientEngineFactory.configureEngine(engine, config)
    engine
  }
}
