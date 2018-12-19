package com.twitter.finagle.netty4.ssl.server

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.netty4.ssl.ContextReloader
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerEngineFactory}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Duration
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.OpenSsl

/**
 * An [[SslServerEngineFactory]] which updates the backing [[io.netty.handler.ssl.SslContext]] at
 * `reloadPeriod` intervals
 */
final private[finagle] class ReloadingNetty4ServerEngineFactory(
  config: SslServerConfiguration,
  allocator: ByteBufAllocator,
  forceJdk: Boolean,
  reloadPeriod: Duration = 1.minute)
    extends SslServerEngineFactory {

  private[this] val reloader = new ContextReloader(
    Netty4ServerSslConfigurations.createServerContext(config, forceJdk),
    DefaultTimer,
    reloadPeriod
  )

  def this(config: SslServerConfiguration) =
    this(config, Allocator.allocatorParam.default.allocator, !OpenSsl.isAvailable)

  def apply(config: SslServerConfiguration): Engine =
    Netty4ServerSslConfigurations.createServerEngine(config, reloader.sslContext, allocator)
}
