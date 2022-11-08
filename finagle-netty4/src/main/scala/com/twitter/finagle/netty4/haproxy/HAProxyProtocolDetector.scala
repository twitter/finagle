package com.twitter.finagle.netty4.haproxy

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.ProtocolDetectionState
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder

private[finagle] object HAProxyProtocolDetector {
  val HandlerName: String = "haproxyDetector"
}

private[finagle] class HAProxyProtocolDetector extends ChannelInboundHandlerAdapter{
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case buf: ByteBuf if HAProxyMessageDecoder.detectProtocol(buf).state() == ProtocolDetectionState.DETECTED =>
        ctx.pipeline()
          .addAfter(HAProxyProtocolDetector.HandlerName, HAProxyProtocolHandler.HandlerName, new HAProxyProtocolHandler())
          .replace(this, "haproxyDecoder", new HAProxyMessageDecoder())
      case _ =>
        ctx.pipeline().remove(this)
    }

    ctx.fireChannelRead(msg)
  }
}
