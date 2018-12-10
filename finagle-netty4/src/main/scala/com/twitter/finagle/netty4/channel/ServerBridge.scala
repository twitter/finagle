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
  serveTransport: Transport[In, Out] { type Context <: Ctx } => Unit)
    extends ChannelInboundHandlerAdapter {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    // We immediately eject ourselves from the pipeline as we don't have
    // any more value to provide.
    ctx.pipeline.remove(this)
    val transport = transportFac(ctx.channel())
    serveTransport(transport)
  }
}
