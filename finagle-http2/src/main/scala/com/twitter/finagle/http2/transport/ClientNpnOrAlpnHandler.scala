package com.twitter.finagle.http2.transport

import com.twitter.finagle.param.Stats
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.http.exp.{HttpCodecName, initClient}
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.ssl.{ApplicationProtocolNames, ApplicationProtocolNegotiationHandler}

private[http2] class ClientNpnOrAlpnHandler(connectionHandler: ChannelHandler, params: Stack.Params)
  extends ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  protected def configurePipeline(ctx: ChannelHandlerContext, protocol: String): Unit =
    protocol match {
      case ApplicationProtocolNames.HTTP_2 =>
        val p = ctx.pipeline
        p.addBefore(
          "buffer",
          "aggregate",
          new AdapterProxyChannelHandler({ pipeline =>
            pipeline.addLast("schemifier", new SchemifyingHandler("https"))
            initClient(params)(pipeline)
          })
        )
        p.addBefore("aggregate", HttpCodecName, connectionHandler)
        upgradeCounter.incr()
        ctx.fireChannelRead(UpgradeEvent.UPGRADE_SUCCESSFUL)

      case ApplicationProtocolNames.HTTP_1_1 =>
        ctx.fireChannelRead(UpgradeEvent.UPGRADE_REJECTED)
        ctx.channel.config.setAutoRead(false)

      case _ =>
        throw new IllegalStateException("unknown protocol: " + protocol)
    }
}
