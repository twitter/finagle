package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.netty4.ssl.Netty4SslConfigurations
import com.twitter.finagle.ssl._
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory}
import com.twitter.util.{Try, Return, Throw}
import com.twitter.util.security.{PrivateKeyFile, X509CertificateFile}
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.{OpenSsl, SslContext, SslContextBuilder}
import javax.net.ssl.SSLEngine

/**
 * This engine factory uses Netty 4's `SslContextBuilder`. It is the
 * recommended path for using native SSL/TLS engines with Finagle.
 */
class Netty4ClientEngineFactory(allocator: ByteBufAllocator, forceJdk: Boolean)
    extends SslClientEngineFactory {

  private[this] def mkSslEngine(
    context: SslContext,
    address: Address,
    config: SslClientConfiguration
  ): SSLEngine =
    address match {
      case Address.Inet(isa, _) =>
        context.newEngine(allocator, SslClientEngineFactory.getHostname(isa, config), isa.getPort)
      case _ =>
        context.newEngine(allocator)
    }

  private[this] def addKey(
    builder: SslContextBuilder,
    keyCredentials: KeyCredentials
  ): Try[SslContextBuilder] =
    keyCredentials match {
      case KeyCredentials.Unspecified =>
        Return(builder) // Do Nothing
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        for {
          key <- new PrivateKeyFile(keyFile).readPrivateKey()
          cert <- new X509CertificateFile(certFile).readX509Certificate()
        } yield builder.keyManager(key, cert)
      case KeyCredentials.CertKeyAndChain(certFile, keyFile, chainFile) =>
        for {
          key <- new PrivateKeyFile(keyFile).readPrivateKey()
          cert <- new X509CertificateFile(certFile).readX509Certificate()
          chain <- new X509CertificateFile(chainFile).readX509Certificates()
        } yield builder.keyManager(key, cert +: chain: _*)
      case KeyCredentials.KeyAndCertChain(key, chain) =>
        Return(builder.keyManager(key, chain:_*))
    }

  /**
   * Creates a new `Engine` based on an `Address` and an `SslClientConfiguration`.
   *
   * @param address A physical address which potentially includes metadata.
   *
   * @param config A collection of parameters which the engine factory should
   * consider when creating the TLS client `Engine`.
   *
   * @note Using `TrustCredentials.Insecure` forces the underlying engine to be
   * a JDK engine and not a native engine, based on what Netty supports.
   *
   * @note `ApplicationProtocols` other than Unspecified are only supported
   * by using a native engine via netty-tcnative.
   */
  def apply(address: Address, config: SslClientConfiguration): Engine = {
    val builder = SslContextBuilder.forClient()
    val withKey = addKey(builder, config.keyCredentials) match {
      case Return(builderWithKey) => builderWithKey
      case Throw(ex) => throw new SslConfigurationException(ex.getMessage, ex)
    }
    val withProvider = Netty4SslConfigurations.configureProvider(withKey, forceJdk)
    val withTrust = Netty4SslConfigurations.configureTrust(withProvider, config.trustCredentials)
    val withAppProtocols = Netty4SslConfigurations.configureClientApplicationProtocols(
      withTrust,
      config.applicationProtocols)
    val context = withAppProtocols.build()
    val engine = new Engine(mkSslEngine(context, address, config))
    SslClientEngineFactory.configureEngine(engine, config)
    engine
  }

}

object Netty4ClientEngineFactory {

  /**
   * Creates an instance of the [[Netty4ClientEngineFactory]] using the
   * allocator defined for use by default in Finagle-Netty4.
   *
   * @param forceJdk Indicates whether the underlying `SslProvider` should
   * be forced to be the Jdk version instead of the native version if
   * available.
   */
  def apply(forceJdk: Boolean): Netty4ClientEngineFactory = {
    val allocator = Allocator.allocatorParam.default.allocator
    new Netty4ClientEngineFactory(allocator, forceJdk)
  }

  /**
   * Creates an instance of the [[Netty4ClientEngineFactory]] using the
   * specified allocator.
   *
   * @param allocator The allocator which should be used as part of
   * `Engine` creation. See Netty's `SslContextBuilder` docs for
   * more information.
   *
   * @note Whether this engine factory should be forced to use the
   * Jdk version is determined by whether Netty is able to load
   * a native engine library via netty-tcnative.
   */
  def apply(allocator: ByteBufAllocator): Netty4ClientEngineFactory =
    new Netty4ClientEngineFactory(allocator, !OpenSsl.isAvailable)

  /**
   * Creates an instance of the [[Netty4ClientEngineFactory]] using the
   * default allocator.
   *
   * @note Whether this engine factory should be forced to use the
   * Jdk version is determined by whether Netty is able to load
   * a native engine library via netty-tcnative.
   */
  def apply(): Netty4ClientEngineFactory =
    apply(!OpenSsl.isAvailable)

}
