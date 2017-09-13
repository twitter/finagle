package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerEngineFactory, SslServerSessionVerifier}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Return, Throw}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.DecoderException
import com.twitter.finagle.netty4.ssl.ServerNameKey
import io.netty.handler.ssl.{AbstractSniHandler, SniHandler, SslContext, SslHandler}
import io.netty.util.{AsyncMapping, ReferenceCountUtil}
import io.netty.util.concurrent.{Future => NettyFuture, Promise}

/**
  * Delays channelActive event till ClientHello with SNI extension is processed, replaces itself with a SslHandler
  * to continue the handshake.
  */
class Netty4SniHandler(mapping: SniSupport.ServerNameToContext, factory: SslServerEngineFactory,
                       verifier: SslServerSessionVerifier) extends AbstractSniHandler[Option[SslServerConfiguration]] {

  override def lookup(ctx: ChannelHandlerContext, hostname: String): NettyFuture[Option[SslServerConfiguration]] = {
    val promise = ctx.executor().newPromise[Option[SslServerConfiguration]]
    mapping(hostname).respond {
      case Return(x)    => promise.setSuccess(x)
      case Throw(exception) => promise.setFailure(exception)
    }
    promise
  }

  override def onLookupComplete(
    ctx: ChannelHandlerContext,
    hostname: String,
    future: NettyFuture[Option[SslServerConfiguration]]
  ): Unit = {
    if (!future.isSuccess()) {
      throw new DecoderException("failed to get the SslContext for " + hostname, future.cause())
    }

    ctx.channel().attr(ServerNameKey).set(hostname)
    future.getNow match {
      case None =>
        val engine = new Engine(SniSupport.DenialSslContext.newEngine(ctx.channel().alloc()))
        replaceHandler(ctx, SslServerConfiguration(), new SslHandler(engine.self))

      case Some(config) =>
        val engine: Engine = factory(config)
        val sslHandler: SslHandler = new SslHandler(engine.self)
        replaceHandler(ctx, config, sslHandler)
    }

  }

  def replaceHandler(ctx: ChannelHandlerContext, config: SslServerConfiguration, sslHandler: SslHandler): Unit = {
    // this method may get called after a client has already disconnected
    val pipeline = ctx.pipeline()
    try {
      pipeline.addAfter("sni", "sslConnect", new SslServerVerificationHandler(sslHandler, config, verifier))
      pipeline.replace(this, "ssl", sslHandler)
    } finally {
      // Since the SslHandler was not inserted into the pipeline the ownership of the SSLEngine was not
      // transferred to the SslHandler.
      // See https://github.com/netty/netty/issues/5678
      if (pipeline.get(classOf[SslHandler]) != sslHandler) {
        ReferenceCountUtil.safeRelease(sslHandler.engine())
      }
    }
    ctx.fireChannelActive()
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val channel = ctx.channel()
    if (!channel.config().isAutoRead) {
      channel.read()
    }
  }

}
