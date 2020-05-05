package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.netty4.ssl.{Alpn, Netty4SslHandler}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.ssl.{ApplicationProtocols, Engine}
import com.twitter.finagle.ssl.server.{
  SslServerConfiguration,
  SslServerEngineFactory,
  SslServerSessionVerifier
}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Address, Stack}
import io.netty.channel.{Channel, ChannelInitializer, ChannelPipeline}
import io.netty.handler.ssl.SslHandler
import java.net.InetSocketAddress

/**
 * A channel initializer that takes [[Stack.Params]] and upgrades the pipeline with missing
 * SSL/TLS pieces required for server-side transport encryption.
 */
final private[finagle] class Netty4ServerSslChannelInitializer(params: Stack.Params)
    extends ChannelInitializer[Channel] {

  import Netty4ServerSslChannelInitializer._

  /**
   * Read the configured `SslServerEngineFactory` out of the stack param.
   * The default for servers is `JdkServerEngineFactory`. If it's configured
   * to use the default, for Netty 4, we replace it with the [[Netty4ServerEngineFactory]]
   * instead.
   */
  private[this] def selectEngineFactory(ch: Channel): SslServerEngineFactory = {
    val defaultEngineFactory = SslServerEngineFactory.Param.param.default.factory
    val engineFactory = params[SslServerEngineFactory.Param].factory

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
    val protocols = params[Alpn].protocols

    config.copy(
      applicationProtocols = ApplicationProtocols.combine(protocols, config.applicationProtocols)
    )
  }

  private[this] def createSslHandler(engine: Engine): SslHandler = {
    // Rip the `SSLEngine` out of the wrapper `Engine` and use it to
    // create an `SslHandler`.
    val statsReceiver = params[Stats].statsReceiver.scope("tls")
    new Netty4SslHandler(engine, statsReceiver)
  }

  private[this] def createSslConnectHandler(
    sslHandler: SslHandler,
    remoteAddress: Address,
    config: SslServerConfiguration
  ): SslServerVerificationHandler = {
    val sessionVerifier = params[SslServerSessionVerifier.Param].verifier
    new SslServerVerificationHandler(sslHandler, remoteAddress, config, sessionVerifier)
  }

  private[this] def addHandlersToPipeline(
    pipeline: ChannelPipeline,
    sslHandler: SslHandler,
    sslConnectHandler: SslServerVerificationHandler
  ): Unit = {
    val name = pipeline.context(this).name
    pipeline.addBefore(name, SslHandlerName, sslHandler)
    pipeline.addBefore(name, SslConnectHandlerName, sslConnectHandler)
  }

  /**
   * In this method, an `Engine` is created by an `SslServerEngineFactory` via
   * an `SslServerConfiguration`. The `Engine` is then used to create the appropriate
   * Netty handler, and it is subsequently added to the channel pipeline.
   */
  def initChannel(ch: Channel): Unit = {
    val remoteAddress: Address =
      // guard against disconnected sessions and test environments with embedded channels
      if (ch.remoteAddress == null || !ch.remoteAddress.isInstanceOf[InetSocketAddress])
        Address.failing
      else Address(ch.remoteAddress.asInstanceOf[InetSocketAddress])

    val configuration = params[Transport.ServerSsl].sslServerConfiguration

    for (config <- configuration) {
      val factory: SslServerEngineFactory = selectEngineFactory(ch)
      val combined: SslServerConfiguration = combineApplicationProtocols(config)
      val engine: Engine = factory(combined)
      val sslHandler: SslHandler = createSslHandler(engine)
      val sslConnectHandler: SslServerVerificationHandler =
        createSslConnectHandler(sslHandler, remoteAddress, combined)

      addHandlersToPipeline(ch.pipeline, sslHandler, sslConnectHandler)
    }
  }
}

private[finagle] object Netty4ServerSslChannelInitializer {

  /** Name that should be used when inserting this handler into a Netty pipeline */
  val HandlerName: String = "tlsInit"

  /** Name of the Netty SslHandler */
  private val SslHandlerName: String = "ssl"

  /** Name of the Connection handler */
  private val SslConnectHandlerName: String = "sslConnect"
}
