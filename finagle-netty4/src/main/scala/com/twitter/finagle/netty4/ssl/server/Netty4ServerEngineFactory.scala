package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerEngineFactory}
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.{OpenSsl, SslContext}

/**
 * This engine factory uses Netty 4's `SslContextBuilder`. It is the
 * recommended path for using native SSL/TLS engines within Finagle.
 */
final class Netty4ServerEngineFactory(allocator: ByteBufAllocator, forceJdk: Boolean)
    extends SslServerEngineFactory {

  /**
   * Creates a new `Engine` based on an `SslServerConfiguration`.
   *
   * @param config A collection of parameters which the engine factory
   * should consider when creating the TLS server `Engine`.
   * @note `KeyCredentials` must be specified, as it is not possible
   * with this engine to use one of the "installed security providers".
   * @note `ApplicationProtocols` other than Unspecified are only supported
   * by using a native engine via netty-tcnative.
   */
  def apply(config: SslServerConfiguration): Engine = {
    val context: SslContext = Netty4ServerSslConfigurations.createServerContext(config, forceJdk)
    Netty4ServerSslConfigurations.createServerEngine(config, context, allocator)
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
