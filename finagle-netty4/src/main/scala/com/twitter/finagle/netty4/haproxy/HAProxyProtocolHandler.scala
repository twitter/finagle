package com.twitter.finagle.netty4.haproxy

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.haproxy.HAProxyMessage

private[finagle] object HAProxyProtocolHandler {
  val HandlerName: String = "haproxyHandler"
}

private[finagle] class HAProxyProtocolHandler extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case proxyMsg: HAProxyMessage =>
        println("------------------------ HA PROXY MESSAGE --------------------------------------", proxyMsg)
        // TODO

        proxyMsg.release()
        ctx.pipeline().remove(this)
      case _ =>
        ctx.fireChannelRead(msg)
    }
  }
}
