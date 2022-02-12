package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.netty4.ssl.FinalizedSslContext
import com.twitter.finagle.netty4.ssl.Netty4SslConfigurations
import com.twitter.finagle.ssl.ApplicationProtocols
import com.twitter.finagle.ssl.CipherSuites
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.KeyCredentials
import com.twitter.finagle.ssl.SslConfigurationException
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.server.SslServerEngineFactory
import com.twitter.util.Return
import com.twitter.util.security.PrivateKeyFile
import com.twitter.util.security.X509CertificateFile
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol
import io.netty.handler.ssl.SslContext
import io.netty.handler.ssl.SslContextBuilder
import scala.collection.JavaConverters._

/**
 * Convenience functions for setting values on a Netty `SslContextBuilder`
 * which are applicable to server configurations and engines.
 */
private[finagle] object Netty4ServerSslConfigurations {

  /**
   * Configures the application protocols of the `SslContextBuilder`. This
   * method mutates the `SslContextBuilder`, and returns it as the result.
   *
   * @note This sets which application level protocol negotiation to
   * use NPN and ALPN.
   *
   * @note This also sets the `SelectorFailureBehavior` to NO_ADVERTISE,
   * and the `SelectedListenerFailureBehavior` to ACCEPT as those are the
   * only modes supported by both JDK and Native engines.
   */
  private def configureServerApplicationProtocols(
    builder: SslContextBuilder,
    applicationProtocols: ApplicationProtocols
  ): SslContextBuilder =
    Netty4SslConfigurations.configureApplicationProtocols(
      builder,
      applicationProtocols,
      Protocol.NPN_AND_ALPN
    )

  /**
   * Creates an `SslContextBuilder` for a server with the supplied `KeyCredentials`.
   *
   * @note `KeyCredentials` must be specified, using `Unspecified` is not supported.
   * @note An `SslConfigurationException` will be thrown if there is an issue loading
   * the certificate(s) or private key.
   *
   * @note Will not validate the validity for certificates when configured
   *       with [[KeyCredentials.KeyManagerFactory]] in contrast to when
   *       configured with [[KeyCredentials.CertAndKey]], [[KeyCredentials.CertsAndKey]],
   *       or [[KeyCredentials.CertKeyAndChain]].
   */
  private def startServerWithKey(keyCredentials: KeyCredentials): SslContextBuilder = {
    val builder = keyCredentials match {
      case KeyCredentials.Unspecified =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.Unspecified",
          "Netty4ServerEngineFactory"
        )
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        for {
          key <- new PrivateKeyFile(keyFile).readPrivateKey()
          cert <- new X509CertificateFile(certFile).readX509Certificate()
        } yield SslContextBuilder.forServer(key, cert)
      case KeyCredentials.CertsAndKey(certsFile, keyFile) =>
        for {
          key <- new PrivateKeyFile(keyFile).readPrivateKey()
          certs <- new X509CertificateFile(certsFile).readX509Certificates()
        } yield SslContextBuilder.forServer(key, certs: _*)
      case KeyCredentials.CertKeyAndChain(certFile, keyFile, chainFile) =>
        for {
          key <- new PrivateKeyFile(keyFile).readPrivateKey()
          cert <- new X509CertificateFile(certFile).readX509Certificate()
          chain <- new X509CertificateFile(chainFile).readX509Certificates()
        } yield SslContextBuilder.forServer(key, cert +: chain: _*)
      case KeyCredentials.KeyManagerFactory(keyManagerFactory) =>
        Return(SslContextBuilder.forServer(keyManagerFactory))
    }
    Netty4SslConfigurations.unwrapTryContextBuilder(builder)
  }

  /**
   * Creates an `SslContext` based on the supplied `SslServerConfiguration`. This method uses
   * the `KeyCredentials`, `TrustCredentials`, `CipherSuites`, and `ApplicationProtocols` from the provided
   * configuration, and forces the JDK provider if forceJdk is true.
   */
  def createServerContext(config: SslServerConfiguration, forceJdk: Boolean): SslContext = {
    val builder = startServerWithKey(config.keyCredentials)
    val withProvider = Netty4SslConfigurations.configureProvider(builder, forceJdk)
    val withTrust = Netty4SslConfigurations.configureTrust(withProvider, config.trustCredentials)
    val withCiphers = config.cipherSuites match {
      case CipherSuites.Enabled(s) => withTrust.ciphers(s.asJava)
      case _ => withTrust
    }
    val withAppProtocols = Netty4ServerSslConfigurations.configureServerApplicationProtocols(
      withCiphers,
      config.applicationProtocols
    )

    // We only want to use the `FinalizedSslContext` if we're using the non-JDK implementation.
    if (!forceJdk) new FinalizedSslContext(withAppProtocols.build())
    else withAppProtocols.build()
  }

  /**
   * Creates an `Engine` based on the supplied `SslContext` and `ByteBufAllocator`, and then
   * configures the underlying `SSLEngine` based on the supplied `SslServerConfiguration`.
   */
  def createServerEngine(
    config: SslServerConfiguration,
    context: SslContext,
    allocator: ByteBufAllocator
  ): Engine = {
    val engine = new Engine(context.newEngine(allocator))
    SslServerEngineFactory.configureEngine(engine, config)
    engine
  }

}
