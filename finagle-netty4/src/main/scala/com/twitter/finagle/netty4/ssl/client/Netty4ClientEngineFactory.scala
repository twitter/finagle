package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.Address
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory}
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.OpenSsl

/**
 * This engine factory uses Netty 4's `SslContextBuilder`. It is the
 * recommended path for using native SSL/TLS engines with Finagle.
 */
final class Netty4ClientEngineFactory(allocator: ByteBufAllocator, forceJdk: Boolean)
    extends SslClientEngineFactory {

  /**
   * Creates a new `Engine` based on an `Address` and an `SslClientConfiguration`.
   *
   * @param address A physical address which potentially includes metadata.
   *
   * @param config A collection of parameters which the engine factory should
   * consider when creating the TLS client `Engine`.
   *
   * @note `ApplicationProtocols` other than Unspecified are only supported
   * by using a native engine via netty-tcnative.
   */
  def apply(address: Address, config: SslClientConfiguration): Engine = {
    val context = Netty4ClientSslConfigurations.createClientContext(config, forceJdk)
    val engine =
      Netty4ClientSslConfigurations.createClientEngine(address, config, context, allocator)
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
