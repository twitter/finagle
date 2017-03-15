package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.netty4.ssl.Alpn
import com.twitter.finagle.ssl.{ApplicationProtocols, Engine}
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerEngineFactory}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Stack
import io.netty.channel.{Channel, ChannelInitializer, ChannelPipeline}
import io.netty.handler.ssl.SslHandler

/**
 * A channel handler that takes [[Stack.Params]] and upgrades the pipeline with missing
 * TLS/SSL pieces required for server-side transport encryption.
 *
 * No matter if the underlying pipeline has been modified or not (or exception was thrown), this
 * handler removes itself from the pipeline on `handlerAdded`.
 */
private[netty4] class Netty4ServerSslHandler(
    params: Stack.Params)
  extends ChannelInitializer[Channel] {

  /**
   * Read the configured `SslServerEngineFactory` out of the stack param.
   * The default for servers is `JdkServerEngineFactory`. If it's configured
   * to use the default, for Netty 4, we replace it with the [[Netty4ServerEngineFactory]]
   * instead.
   */
  private[this] def selectEngineFactory(ch: Channel): SslServerEngineFactory = {
    val SslServerEngineFactory.Param(defaultEngineFactory) =
      SslServerEngineFactory.Param.param.default
    val SslServerEngineFactory.Param(engineFactory) =
      params[SslServerEngineFactory.Param]

    if (engineFactory == defaultEngineFactory) Netty4ServerEngineFactory(ch.alloc())
    else engineFactory
  }

  /**
   * This method combines `ApplicationProtocols` that may have been set by the user
   * with ones that are set based on using a protocol like HTTP/2.
   */
  private[this] def combineApplicationProtocols(
    config: SslServerConfiguration
  ): SslServerConfiguration = {
    val Alpn(protocols) = params[Alpn]

    config.copy(applicationProtocols =
      ApplicationProtocols.combine(protocols, config.applicationProtocols))
  }

  private[this] def createSslHandler(engine: Engine): SslHandler =
    // Rip the `SSLEngine` out of the wrapper `Engine` and use it to
    // create an `SslHandler`.
    new SslHandler(engine.self)

  private[this] def addHandlerToPipeline(pipeline: ChannelPipeline, sslHandler: SslHandler): Unit =
    pipeline.addFirst("ssl", sslHandler)

  /**
   * In this method, an `Engine` is created by an `SslServerEngineFactory` via
   * an `SslServerConfiguration`. The `Engine` is then used to create the appropriate
   * Netty handler, and it is subsequently added to the channel pipeline.
   */
  def initChannel(ch: Channel): Unit = {
    val Transport.ServerSsl(configuration) = params[Transport.ServerSsl]

    for (config <- configuration) {
      val factory: SslServerEngineFactory = selectEngineFactory(ch)
      val combined: SslServerConfiguration = combineApplicationProtocols(config)
      val engine: Engine = factory(combined)
      val sslHandler: SslHandler = createSslHandler(engine)
      addHandlerToPipeline(ch.pipeline(), sslHandler)
    }
  }
}
