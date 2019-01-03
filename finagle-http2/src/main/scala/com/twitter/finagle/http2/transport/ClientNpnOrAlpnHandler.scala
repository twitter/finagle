package com.twitter.finagle.http2.transport

import com.twitter.finagle.param.Stats
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.http.{
  HttpCodecName,
  initClient,
  initClientBefore,
  newHttpClientCodec
}
import com.twitter.finagle.netty4.transport.ChannelTransport
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.ssl.{ApplicationProtocolNames, ApplicationProtocolNegotiationHandler}

private[http2] class ClientNpnOrAlpnHandler(connectionHandler: ChannelHandler, params: Stack.Params)
    extends ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  protected def configurePipeline(ctx: ChannelHandlerContext, protocol: String): Unit = {
    val pipeline = ctx.pipeline
    protocol match {
      case ApplicationProtocolNames.HTTP_2 =>
        pipeline.addBefore(
          BufferingHandler.HandlerName,
          AdapterProxyChannelHandler.HandlerName,
          new AdapterProxyChannelHandler(
            { p =>
              p.addLast(SchemifyingHandler.HandlerName, new SchemifyingHandler("https"))
              p.addLast(StripHeadersHandler.HandlerName, StripHeadersHandler)
              initClient(params)(p)
            },
            statsReceiver.scope("adapter_proxy")
          )
        )
        pipeline.addBefore(AdapterProxyChannelHandler.HandlerName, HttpCodecName, connectionHandler)
        upgradeCounter.incr()
        ctx.fireChannelRead(UpgradeEvent.UPGRADE_SUCCESSFUL)

      case ApplicationProtocolNames.HTTP_1_1 =>
        // We unset the limit for maxChunkSize (8k by default) so Netty emits entire available
        // payload as a single chunk instead of splitting it. This way we put the data into use
        // quicker, as soon as it's available.
        pipeline.addBefore(ChannelTransport.HandlerName, HttpCodecName, newHttpClientCodec(params))
        pipeline.remove(BufferingHandler.HandlerName)
        initClientBefore(ChannelTransport.HandlerName, params)(pipeline)
        ctx.channel.config.setAutoRead(false)
        ctx.fireChannelRead(UpgradeEvent.UPGRADE_REJECTED)
      case _ =>
        throw new IllegalStateException("unknown protocol: " + protocol)
    }
  }
}
