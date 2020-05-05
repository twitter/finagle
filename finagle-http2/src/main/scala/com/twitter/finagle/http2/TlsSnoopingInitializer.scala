package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.ssl.server.Netty4TlsSnoopingHandler
import io.netty.channel.{
  Channel,
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter,
  ChannelInitializer,
  ChannelPipeline
}

// A initializer that waits for the result of the TLS snooping and
// decides whether to spin up the TLS-ALPN or to begin cleartext H2
private[http2] class TlsSnoopingInitializer(init: ChannelInitializer[Channel], params: Stack.Params)
    extends ChannelInitializer[Channel] {

  private class SnooperSignalHandler extends ChannelInboundHandlerAdapter {

    // We rely on the signal from the `Netty4TlsSnoopingHandler` to determine whether
    // to initialize the pipeline as HTTP/2 cleartext (H2C or prior knowledge)
    // or HTTP/(1|2) ALPN.
    override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit = {
      val handled = evt match {
        case Netty4TlsSnoopingHandler.Result.Secure =>
          ctx.pipeline.addAfter(
            ctx.name,
            "serverInitializer",
            new Http2TlsServerInitializer(init, params))
          true

        case Netty4TlsSnoopingHandler.Result.Cleartext =>
          ctx.pipeline.addAfter(
            ctx.name,
            "serverInitializer",
            new Http2CleartextServerInitializer(init, params))
          true

        case _ =>
          false
      }

      ctx.fireUserEventTriggered(evt)
      if (handled) {
        ctx.pipeline.remove(this)
      }
    }
  }

  def initChannel(ch: Channel): Unit = {
    val p = ch.pipeline
    val lastHandler =
      if (snoopingEnabled(p)) new SnooperSignalHandler
      else new Http2TlsServerInitializer(init, params)

    p.addLast(lastHandler)
  }

  private[this] def snoopingEnabled(pipeline: ChannelPipeline): Boolean =
    pipeline.get(classOf[Netty4TlsSnoopingHandler]) != null
}
