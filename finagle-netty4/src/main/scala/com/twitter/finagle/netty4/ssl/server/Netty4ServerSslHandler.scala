package com.twitter.finagle.netty4.ssl.server

import java.util
import java.security.KeyStore
import javax.net.ssl._

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.ssl.Alpn
import com.twitter.finagle.ssl.{ApplicationProtocols, Engine}
import com.twitter.finagle.ssl.server.{
  SslServerConfiguration,
  SslServerEngineFactory,
  SslServerSessionVerifier
}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future => TwitterFuture}
import io.netty.buffer.ByteBufAllocator
import io.netty.channel._
import io.netty.handler.ssl._

final case class SniSupport(mapping: SniSupport.ServerNameToContext)

object SniSupport {

  type ServerNameToContext = String => TwitterFuture[Option[SslServerConfiguration]]

  implicit val param: Stack.Param[SniSupport] = Stack.Param(SniSupport( (_: String) => {
    TwitterFuture.exception(new IllegalStateException("sni support without mapping makes no sense"))
  }))

  private object DenialSNIMatcher extends SNIMatcher(StandardConstants.SNI_HOST_NAME) {
    override def matches(sniServerName: SNIServerName): Boolean = false
  }

  /**
    * Uses JDK SslContext to create an engine rejecting all host names. Is needed to
    * send the 'unrecognized_name' alert to the client.
    */
  private[ssl] val DenialSslContext = {
    new SslContext() {

      val delegate = {
        val emptyKeyStore = KeyStore.getInstance(KeyStore.getDefaultType)
        emptyKeyStore.load(null, Array.empty)
        val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        kmf.init(emptyKeyStore, Array.empty)
        SslContextBuilder.forServer(kmf).sslProvider(SslProvider.JDK).build()
      }
      val matchers: util.Collection[SNIMatcher] = util.Collections.singleton(DenialSNIMatcher)
      private def setSNIMatcher(engine: SSLEngine): SSLEngine = {
        val params = engine.getSSLParameters
        params.setSNIMatchers(matchers)
        engine.setSSLParameters(params)
        engine
      }

      override def sessionContext(): SSLSessionContext = delegate.sessionContext()

      override def newEngine(alloc: ByteBufAllocator): SSLEngine = {
        setSNIMatcher(delegate.newEngine(alloc))
      }

      override def newEngine(alloc: ByteBufAllocator, peerHost: String, peerPort: Int): SSLEngine = {
        setSNIMatcher(delegate.newEngine(alloc, peerHost, peerPort))
      }

      override def isClient: Boolean = false

      override def applicationProtocolNegotiator(): ApplicationProtocolNegotiator = delegate.applicationProtocolNegotiator()

      override def sessionCacheSize(): Long = delegate.sessionCacheSize()

      override def cipherSuites(): util.List[String] = delegate.cipherSuites()

      override def sessionTimeout(): Long = delegate.sessionTimeout()
    }

  }

  def fromOption(mapping: String => Option[SslServerConfiguration]): SniSupport = SniSupport((serverName: String) => {
    TwitterFuture.value(mapping(serverName))
  })
}

/**
 * A channel handler that takes [[Stack.Params]] and upgrades the pipeline with missing
 * SSL/TLS pieces required for server-side transport encryption.
 *
 * No matter if the underlying pipeline has been modified or not (or exception was thrown), this
 * handler removes itself from the pipeline on `handlerAdded`.
 */
private[finagle] class Netty4ServerSslHandler(params: Stack.Params)
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

    config.copy(
      applicationProtocols = ApplicationProtocols.combine(protocols, config.applicationProtocols)
    )
  }

  private[this] def createSslHandler(engine: Engine): SslHandler =
    // Rip the `SSLEngine` out of the wrapper `Engine` and use it to
    // create an `SslHandler`.
    new SslHandler(engine.self)

  private[this] def createSslConnectHandler(
    sslHandler: SslHandler,
    config: SslServerConfiguration
  ): SslServerVerificationHandler = {
    val SslServerSessionVerifier.Param(sessionVerifier) = params[SslServerSessionVerifier.Param]
    new SslServerVerificationHandler(sslHandler, config, sessionVerifier)
  }

  private[this] def addHandlersToPipeline(
    pipeline: ChannelPipeline,
    sslHandler: SslHandler,
    sslConnectHandler: SslServerVerificationHandler
  ): Unit = {
    pipeline.addFirst("sslConnect", sslConnectHandler)
    pipeline.addFirst("ssl", sslHandler)
  }

  private[this] def addSniHandler(channel: Channel, factory: SslServerEngineFactory): Unit = {
    val SniSupport(mapping) = params[SniSupport]
    val SslServerSessionVerifier.Param(sessionVerifier) = params[SslServerSessionVerifier.Param]
    channel.pipeline().addFirst("sni", new Netty4SniHandler(mapping, factory, sessionVerifier))
  }

  /**
   * In this method, an `Engine` is created by an `SslServerEngineFactory` via
   * an `SslServerConfiguration`. The `Engine` is then used to create the appropriate
   * Netty handler, and it is subsequently added to the channel pipeline.
   */
  def initChannel(ch: Channel): Unit = {
    val Transport.ServerSsl(configuration) = params[Transport.ServerSsl]

    for (config <- configuration) {
      val factory: SslServerEngineFactory = selectEngineFactory(ch)
      if( params.contains[SniSupport] ) {
        addSniHandler(ch, factory)
      } else {
        val combined: SslServerConfiguration = combineApplicationProtocols(config)
        val engine: Engine = factory(combined)
        val sslHandler: SslHandler = createSslHandler(engine)
        val sslConnectHandler: SslServerVerificationHandler = createSslConnectHandler(sslHandler, combined)
        addHandlersToPipeline(ch.pipeline, sslHandler, sslConnectHandler)
      }
    }
  }

}
