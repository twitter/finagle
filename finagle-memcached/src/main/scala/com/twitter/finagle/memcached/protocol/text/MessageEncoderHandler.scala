package com.twitter.finagle.memcached.protocol.text

import io.netty.channel.{ChannelHandlerContext, ChannelOutboundHandlerAdapter, ChannelPromise}

/**
 * This encoder handler encodes messages of type `T` to `Buf`s using `encoder`.
 */
private[memcached] class MessageEncoderHandler[T](encoder: MessageEncoder[T])
    extends ChannelOutboundHandlerAdapter {

  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit =
    ctx.write(encoder.encode(msg.asInstanceOf[T]), promise)
}
