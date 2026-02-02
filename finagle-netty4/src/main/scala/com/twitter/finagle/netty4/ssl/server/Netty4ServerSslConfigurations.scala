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
    applicationProtocols: ApplicationProtocols,
    forceJdk: Boolean
  ): SslContextBuilder = {
    // JDK provider does not support NPN_AND_ALPN protocol, so if forceJDK is true we default to ALPN only
    val protocol = if (forceJdk) Protocol.ALPN else Protocol.NPN_AND_ALPN
    Netty4SslConfigurations.configureApplicationProtocols(
      builder,
      applicationProtocols,
      protocol
    )
  }

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
   * Wraps trust credentials to inject service identifier capturing for servers.
   * This allows us to track which clients fail SSL handshakes even when PKIX validation fails.
   *
   * For CertCollection (the common mTLS case), we convert it to a TrustManagerFactory and wrap it.
   * For TrustManagerFactory, we wrap it directly.
   * Other types pass through to preserve existing error handling.
   */
  private def wrapTrustCredentialsForCapture(
    trustCredentials: com.twitter.finagle.ssl.TrustCredentials
  ): com.twitter.finagle.ssl.TrustCredentials = {
    import com.twitter.util.Try
    import com.twitter.util.security.X509CertificateFile

    trustCredentials match {
      case com.twitter.finagle.ssl.TrustCredentials.TrustManagerFactory(factory) =>
        com.twitter.finagle.ssl.TrustCredentials.TrustManagerFactory(
          new ServiceIdentifierCapturingTrustManagerFactory(factory)
        )

      case com.twitter.finagle.ssl.TrustCredentials.CertCollection(file) =>
        // Convert CertCollection to TrustManagerFactory, then wrap it
        // This is the common case for mTLS servers using ca-bundle.crt
        // We use X509CertificateFile for validation, matching Netty's behavior
        val tmfResult = for {
          certs <- new X509CertificateFile(file).readX509Certificates()
          tmf <- Try {
            val factory = javax.net.ssl.TrustManagerFactory.getInstance(
              javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm
            )
            val ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType)
            ks.load(null, null)

            var i = 0
            certs.foreach { cert =>
              ks.setCertificateEntry(s"cert-$i", cert)
              i += 1
            }

            factory.init(ks)
            factory
          }
        } yield tmf

        // If conversion succeeds, wrap it; otherwise pass through original to let Netty handle the error
        tmfResult match {
          case com.twitter.util.Return(tmf) =>
            com.twitter.finagle.ssl.TrustCredentials.TrustManagerFactory(
              new ServiceIdentifierCapturingTrustManagerFactory(tmf)
            )
          case com.twitter.util.Throw(_) =>
            // Conversion failed (e.g., empty file, invalid format)
            // Pass through original CertCollection to preserve existing error handling
            trustCredentials
        }

      case other => other // Unspecified, Insecure, X509Certificates - pass through
    }
  }

  /**
   * Creates an `SslContext` based on the supplied `SslServerConfiguration`. This method uses
   * the `KeyCredentials`, `TrustCredentials`, `CipherSuites`, and `ApplicationProtocols` from the provided
   * configuration, and forces the JDK provider if forceJdk is true.
   */
  def createServerContext(config: SslServerConfiguration, forceJdk: Boolean): SslContext = {
    val builder = startServerWithKey(config.keyCredentials)
    val withProvider = Netty4SslConfigurations.configureProvider(builder, forceJdk)
    // Wrap trust credentials to capture service identifiers during cert validation
    val wrappedTrust = wrapTrustCredentialsForCapture(config.trustCredentials)
    val withTrust = Netty4SslConfigurations.configureTrust(withProvider, wrappedTrust)
    val withCiphers = config.cipherSuites match {
      case CipherSuites.Enabled(s) => withTrust.ciphers(s.asJava)
      case _ => withTrust
    }
    val withAppProtocols = Netty4ServerSslConfigurations.configureServerApplicationProtocols(
      withCiphers,
      config.applicationProtocols,
      forceJdk
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
