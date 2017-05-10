package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.netty4.ssl.Netty4SslConfigurations
import com.twitter.finagle.ssl._
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerEngineFactory}
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.{OpenSsl, SslContextBuilder}

/**
 * This engine factory uses Netty 4's `SslContextBuilder`. It is the
 * recommended path for using native SSL/TLS engines within Finagle.
 */
class Netty4ServerEngineFactory(allocator: ByteBufAllocator, forceJdk: Boolean)
  extends SslServerEngineFactory {

  private[this] def startWithKey(keyCredentials: KeyCredentials): SslContextBuilder =
    keyCredentials match {
      case KeyCredentials.Unspecified =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.Unspecified", "Netty4ServerEngineFactory")
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        SslContextBuilder.forServer(certFile, keyFile)
      case _: KeyCredentials.CertKeyAndChain =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertKeyAndChain", "Netty4ServerEngineFactory")
    }

  /**
   * Creates a new `Engine` based on an `SslServerConfiguration`.
   *
   * @param config A collection of parameters which the engine factory
   * should consider when creating the TLS server `Engine`.
   *
   * @note `KeyCredentials` must be specified, as it is not possible
   * with this engine to use one of the "isntalled security providers".
   *
   * @note Using `TrustCredentials.Insecure` forces the underlying engine to be
   * a JDK engine and not a native engine, based on what Netty supports.
   *
   * @note `ApplicationProtocols` other than Unspecified are only supported
   * by using a native engine via netty-tcnative.
   */
  def apply(config: SslServerConfiguration): Engine = {
    val builder = startWithKey(config.keyCredentials)

    val withProvider = Netty4SslConfigurations.configureProvider(builder, forceJdk)
    val withTrust = Netty4SslConfigurations.configureTrust(withProvider, config.trustCredentials)
    val withAppProtocols = Netty4SslConfigurations.configureApplicationProtocols(
      withTrust, config.applicationProtocols)
    val context = withAppProtocols.build()
    val engine = new Engine(context.newEngine(allocator))
    SslServerEngineFactory.configureEngine(engine, config)
    engine
  }

}

object Netty4ServerEngineFactory {

  /**
   * Creates an instance of the [[Netty4ServerEngineFactory]] using the
   * allocator defined for use by default in Finagle-Netty4.
   *
   * @param forceJdk Indicates whether the underlying `SslProvider` should
   * be forced to be the Jdk version instead of the native version if
   * available.
   */
  def apply(forceJdk: Boolean): Netty4ServerEngineFactory = {
    val allocator = Allocator.allocatorParam.default.allocator
    new Netty4ServerEngineFactory(allocator, forceJdk)
  }

  /**
   * Creates an instance of the [[Netty4ServerEngineFactory]] using the
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
  def apply(allocator: ByteBufAllocator): Netty4ServerEngineFactory =
    new Netty4ServerEngineFactory(allocator, !OpenSsl.isAvailable)

  /**
   * Creates an instance of the [[Netty4ServerEngineFactory]] using the
   * default allocator.
   *
   * @note Whether this engine factory should be forced to use the
   * Jdk version is determined by whether Netty is able to load
   * a native engine library via netty-tcnative.
   */
  def apply(): Netty4ServerEngineFactory =
    apply(!OpenSsl.isAvailable)

}
