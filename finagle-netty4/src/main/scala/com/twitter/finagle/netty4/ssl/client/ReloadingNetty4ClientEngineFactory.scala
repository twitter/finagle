package com.twitter.finagle.netty4.ssl.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.netty4.ssl.ContextReloader
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Duration
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.OpenSsl

/**
 * An [[SslClientEngineFactory]] which updates the backing [[io.netty.handler.ssl.SslContext]] at
 * `reloadPeriod` intervals
 */
final private[finagle] class ReloadingNetty4ClientEngineFactory(
  config: SslClientConfiguration,
  allocator: ByteBufAllocator,
  forceJdk: Boolean,
  reloadPeriod: Duration = 1.minute)
    extends SslClientEngineFactory {

  private[this] val reloader = new ContextReloader(
    Netty4ClientSslConfigurations.createClientContext(config, forceJdk),
    DefaultTimer,
    reloadPeriod
  )

  def this(config: SslClientConfiguration) =
    this(config, Allocator.allocatorParam.default.allocator, !OpenSsl.isAvailable)

  def apply(address: Address, config: SslClientConfiguration): Engine =
    Netty4ClientSslConfigurations.createClientEngine(
      address,
      config,
      reloader.sslContext,
      allocator
    )
}
