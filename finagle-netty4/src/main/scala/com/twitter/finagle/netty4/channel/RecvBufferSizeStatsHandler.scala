package com.twitter.finagle.netty4.channel

import com.twitter.finagle.stats.StatsReceiver
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
 * An inbound channel handler that reports receive buffers sizes in a `receive_buffer_bytes`
 * histogram on a given [[StatsReceiver]].
 */
@Sharable
private[netty4] class RecvBufferSizeStatsHandler(stats: StatsReceiver)
  extends ChannelInboundHandlerAdapter {

  private[this] val receiveBufferBytes = stats.stat("receive_buffer_bytes")

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case bb: ByteBuf => receiveBufferBytes.add(bb.readableBytes().toFloat)
      case _ => // NOOP
    }

    ctx.fireChannelRead(msg)
  }
}
