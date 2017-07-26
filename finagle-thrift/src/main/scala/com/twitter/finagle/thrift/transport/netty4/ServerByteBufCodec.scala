package com.twitter.finagle.thrift.transport.netty4

import com.twitter.finagle.thrift.transport.ExceptionFactory
import io.netty.buffer.Unpooled
import io.netty.channel.{
  ChannelHandler,
  ChannelHandlerContext,
  ChannelOutboundHandlerAdapter,
  ChannelPromise,
  CombinedChannelDuplexHandler
}
import io.netty.channel.ChannelHandler.Sharable

/**
 * Server codec that converts `ByteBuf`s to and from `Array[Byte]`.
 *
 * This codec also handles some signalling, specifically it will complete the
 * `ChannelPromise` associated with encoding an empty `Array[Byte]`.
 */
private[netty4] object ServerByteBufCodec {

  def apply(): ChannelHandler = {
    val encoder = ThriftServerArrayToByteBufEncoder
    val decoder = ThriftByteBufToArrayDecoder
    new CombinedChannelDuplexHandler(decoder, encoder)
  }

  @Sharable
  private object ThriftServerArrayToByteBufEncoder extends ChannelOutboundHandlerAdapter {
    override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit =
      msg match {
        case array: Array[Byte] =>
          val buf = Unpooled.wrappedBuffer(array)
          ctx.writeAndFlush(buf, promise)

        case other =>
          val ex = ExceptionFactory.wrongServerWriteType(other)
          promise.setFailure(ex)
          throw ex
      }
  }
}
