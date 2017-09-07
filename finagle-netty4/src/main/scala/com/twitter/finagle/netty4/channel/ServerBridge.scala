package com.twitter.finagle.netty4.channel

import com.twitter.finagle.transport.{Transport, TransportContext}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.channel.ChannelHandler.Sharable

/**
 * Bridges a `Channel` onto a [[Transport]].
 */
@Sharable
private[netty4] class ServerBridge[In, Out, Ctx <: TransportContext](
  transportFac: Channel => Transport[In, Out] { type Context <: Ctx },
  serveTransport: Transport[In, Out] { type Context <: Ctx } => Unit
) extends ChannelInboundHandlerAdapter {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val transport = transportFac(ctx.channel())
    serveTransport(transport)
  }
}
