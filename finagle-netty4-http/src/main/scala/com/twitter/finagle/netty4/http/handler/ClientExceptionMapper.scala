package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http.TooLongMessageException
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.TooLongFrameException
import io.netty.handler.codec.http.HttpObject
import io.netty.util.ReferenceCountUtil

/**
 * Map some Netty 4 HTTP client related exceptions to Finagle specific exceptions.
 *
 * This handler also accounts for `DecoderResult` possibly being failed and converts malformed
 * HTTP objects into appropriate exceptions.
 *
 * @see [[BadRequestHandler]] for a server-side implementation of this handler.
 */
@Sharable
private[http] object ClientExceptionMapper extends ChannelInboundHandlerAdapter {

  private[this] def toFinagle(ctx: ChannelHandlerContext, t: Throwable): Throwable = t match {
    case e: TooLongFrameException =>
      TooLongMessageException(e, ctx.channel.remoteAddress)

    case _ => t
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    // NOTE: HttpObjectDecoder sets the DecoderResult on an inbound HTTP message when either
    // headers or initial line exceed a desired limit.
    case o: HttpObject if o.decoderResult.isFailure =>
      ReferenceCountUtil.release(o)
      ctx.fireExceptionCaught(toFinagle(ctx, o.decoderResult.cause))

    case _ => ctx.fireChannelRead(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // NOTE: HttpObjectAggregator fires exception when aggregated content exceeds a desired limit.
    ctx.fireExceptionCaught(toFinagle(ctx, cause))
  }
}
