package com.twitter.finagle.thrift.transport.netty4

import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.thrift.transport.ExceptionFactory
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{
  ChannelHandler,
  ChannelHandlerContext,
  ChannelOutboundHandlerAdapter,
  ChannelPromise,
  CombinedChannelDuplexHandler
}

/**
 * Codec that converts outbound client requests to `ByteBuf`s
 * and inbound `ByteBuf`s to arrays of type `Array[Byte]`.
 */
private[netty4] object ClientByteBufCodec {

  def apply(): ChannelHandler = {
    val encoder = ThriftClientArrayToByteBufEncoder
    val decoder = ThriftByteBufToArrayDecoder
    new CombinedChannelDuplexHandler(decoder, encoder)
  }

  /**
   * ThriftClientByteBufEncoder translates ThriftClientRequests to
   * bytes on the wire.
   */
  @Sharable
  private object ThriftClientArrayToByteBufEncoder extends ChannelOutboundHandlerAdapter {

    override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit =
      msg match {
        case request: ThriftClientRequest =>
          val buf = Unpooled.wrappedBuffer(request.message)
          ctx.writeAndFlush(buf, promise)

        case other =>
          val ex = ExceptionFactory.wrongClientWriteType(other)
          promise.setFailure(ex)
          throw ex
      }
  }
}
