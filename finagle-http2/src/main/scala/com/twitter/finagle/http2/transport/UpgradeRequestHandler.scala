package com.twitter.finagle.http2.transport

import io.netty.channel.{ChannelInboundHandlerAdapter, ChannelHandlerContext}
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent

/**
 * Takes the upgrade result and marks it as something read off the wire to
 * expose it to finagle.
 */
private[http2] class UpgradeRequestHandler extends ChannelInboundHandlerAdapter {
  override def userEventTriggered(ctx: ChannelHandlerContext, event: Any): Unit = {
    event match {
      case rejected@UpgradeEvent.UPGRADE_REJECTED =>
        ctx.fireChannelRead(rejected)
        // disable autoread if we fail the upgrade
        ctx.channel.config.setAutoRead(false)
        ctx.pipeline.remove(this)
      case successful@UpgradeEvent.UPGRADE_SUCCESSFUL =>
        ctx.fireChannelRead(successful)
        ctx.pipeline.remove(this)
      case _ => // nop
    }
    super.userEventTriggered(ctx, event)
  }
}
