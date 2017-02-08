package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.netty4.ssl.Netty4SslConfigurations
import com.twitter.finagle.ssl._
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory}
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import javax.net.ssl.SSLEngine

/**
 * This engine factory uses Netty 4's `SslContextBuilder`. It is the
 * recommended path for using native SSL/TLS engines with Finagle.
 */
class Netty4ClientEngineFactory(allocator: ByteBufAllocator)
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
  ): SslContextBuilder =
    keyCredentials match {
      case KeyCredentials.Unspecified =>
        builder // Do Nothing
      case KeyCredentials.CertAndKey(certFile, keyFile) =>
        builder.keyManager(certFile, keyFile)
      case _: KeyCredentials.CertKeyAndChain =>
        throw SslConfigurationException.notSupported(
          "KeyCredentials.CertKeyAndChain", "Netty4ClientEngineFactory")
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
    val withKey = addKey(builder, config.keyCredentials)
    val withTrust = Netty4SslConfigurations.configureTrust(withKey, config.trustCredentials)
    val withAppProtocols = Netty4SslConfigurations.configureApplicationProtocols(
      withTrust, config.applicationProtocols)
    val context = withAppProtocols.build()
    val engine = new Engine(mkSslEngine(context, address, config))
    SslClientEngineFactory.configureEngine(engine, config)
    engine
  }

}

object Netty4ClientEngineFactory {

  /**
   * Creates an instance of the Netty4ClientEngineFactory using the
   * allocator defined for use by default in Finagle-Netty4.
   */
  def apply(): Netty4ClientEngineFactory = {
    val allocator = Allocator.allocatorParam.default.allocator
    new Netty4ClientEngineFactory(allocator)
  }

}
