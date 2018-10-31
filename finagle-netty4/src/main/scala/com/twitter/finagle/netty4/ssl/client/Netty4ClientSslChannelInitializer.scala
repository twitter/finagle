package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.ssl.{Alpn, Netty4SslHandler}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.ssl.{ApplicationProtocols, Engine}
import com.twitter.finagle.ssl.client.{
  SslClientConfiguration,
  SslClientEngineFactory,
  SslClientSessionVerifier
}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Address, Stack}
import com.twitter.util.Try
import io.netty.channel.{Channel, ChannelInitializer, ChannelPipeline}
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}

private[finagle] object Netty4ClientSslChannelInitializer {

  /**
   * A class eligible for configuring a callback that is triggered when the ssl
   * handshake is complete. Note, this is intended for internal use only.
   */
  case class OnSslHandshakeComplete(onComplete: Try[Unit] => Unit)
  object OnSslHandshakeComplete {
    implicit val param = Stack.Param(OnSslHandshakeComplete((_: Try[Unit]) => ()))
  }
}

/**
 * A channel initializer that takes [[Stack.Params]] and upgrades the pipeline with missing
 * SSL/TLS pieces required for client-side transport encryption.
 */
private[finagle] class Netty4ClientSslChannelInitializer(params: Stack.Params)
    extends ChannelInitializer[Channel] {

  // The reason we can't close the channel immediately is because we're in process of decoding an
  // inbound message that is represented by a bunch of TLS records. We need to finish decoding
  // and send that message up to the pipeline before closing the channel. This is why we queue the
  // close event.
  //
  // See CSL-1610 (internal ticket) for more details.
  private[this] val closeChannelOnCloseNotify =
    new GenericFutureListener[NettyFuture[Channel]] {
      def operationComplete(f: NettyFuture[Channel]): Unit = {
        val channel = f.getNow
        if (channel != null && f.isSuccess) {
          channel
            .eventLoop()
            .execute(new Runnable {
              def run(): Unit = channel.close()
            })
        }
      }
    }

  /**
   * Read the configured `SslClientEngineFactory` out of the stack param.
   * The default for clients is `JdkClientEngineFactory`. If it's configured
   * to use the default, for Netty 4, we replace it with the [[Netty4ClientEngineFactory]]
   * instead.
   */
  private[this] def selectEngineFactory(ch: Channel): SslClientEngineFactory = {
    val SslClientEngineFactory.Param(defaultEngineFactory) =
      SslClientEngineFactory.Param.param.default
    val SslClientEngineFactory.Param(engineFactory) =
      params[SslClientEngineFactory.Param]

    if (engineFactory == defaultEngineFactory) Netty4ClientEngineFactory(ch.alloc())
    else engineFactory
  }

  /**
   * This method combines `ApplicationProtocols` that may have been set by the user
   * with ones that are set based on using a protocol like HTTP/2.
   */
  private[this] def combineApplicationProtocols(
    config: SslClientConfiguration
  ): SslClientConfiguration = {
    val Alpn(protocols) = params[Alpn]

    config.copy(
      applicationProtocols = ApplicationProtocols.combine(protocols, config.applicationProtocols)
    )
  }

  private[this] def createSslHandler(engine: Engine): SslHandler = {
    // Rip the `SSLEngine` out of the wrapper `Engine` and use it to
    // create an `SslHandler`.
    val statsReceiver = params[Stats].statsReceiver.scope("tls")
    val ssl = new Netty4SslHandler(engine, statsReceiver)

    // Close channel on close_notify received from a remote peer.
    ssl.sslCloseFuture().addListener(closeChannelOnCloseNotify)

    // Disable Netty's default handshake timeout (10 seconds). We rely on
    // Finagle's own timeouts (i.e., session creation timeout) to interrupt
    // the handshake.
    ssl.setHandshakeTimeoutMillis(0)
    ssl
  }

  private[this] def createSslConnectHandler(
    sslHandler: SslHandler,
    address: Address,
    config: SslClientConfiguration
  ): SslClientVerificationHandler = {
    val SslClientSessionVerifier.Param(sessionVerifier) = params[SslClientSessionVerifier.Param]
    val sslVerifier = new SslClientVerificationHandler(sslHandler, address, config, sessionVerifier)

    val onSslHandshakeComplete =
      params[Netty4ClientSslChannelInitializer.OnSslHandshakeComplete].onComplete
    sslVerifier.handshakeComplete.respond(onSslHandshakeComplete)

    sslVerifier
  }

  private[this] def addHandlersToPipeline(
    pipeline: ChannelPipeline,
    sslHandler: SslHandler,
    sslConnectHandler: SslClientVerificationHandler
  ): Unit = {
    pipeline.addFirst("sslConnect", sslConnectHandler)
    pipeline.addFirst("ssl", sslHandler)
  }

  /**
   * In this method, an `Engine` is created by an `SslClientEngineFactory` via
   * an `SslClientConfiguration` and an `Address`. The `Engine` and the
   * `SslClientConfiguration` are then used to create the appropriate Netty
   * handlers, and they are subsequently added to the channel pipeline.
   */
  def initChannel(ch: Channel): Unit = {
    val Transporter.EndpointAddr(address) = params[Transporter.EndpointAddr]
    val Transport.ClientSsl(configuration) = params[Transport.ClientSsl]

    for (config <- configuration) {
      val factory: SslClientEngineFactory = selectEngineFactory(ch)
      val combined: SslClientConfiguration = combineApplicationProtocols(config)
      val engine: Engine = factory(address, combined)
      val sslHandler: SslHandler = createSslHandler(engine)
      val sslConnectHandler: SslClientVerificationHandler =
        createSslConnectHandler(sslHandler, address, combined)
      addHandlersToPipeline(ch.pipeline, sslHandler, sslConnectHandler)
    }
  }

}
