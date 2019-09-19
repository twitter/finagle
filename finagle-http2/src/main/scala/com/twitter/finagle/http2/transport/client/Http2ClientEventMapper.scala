package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.http.BadRequestResponse
import com.twitter.finagle.netty4.http.Bijections
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{
  ChannelFuture,
  ChannelFutureListener,
  ChannelHandlerContext,
  ChannelOutboundHandlerAdapter,
  ChannelPromise
}
import io.netty.handler.codec.http.{FullHttpResponse, HttpRequest}
import io.netty.handler.codec.http2.Http2Exception.HeaderListSizeException

/**
 * A Netty pipeline stage that converts events to fit the finagle client model
 *
 * This stage translates certain exceptions and modifies the inbound response to
 * trigger closing of the `Transport` since each request will get it's own Netty
 * stream channel from the `Http2MultiplexHandler`.
 */
@Sharable
private[http2] object Http2ClientEventMapper extends ChannelOutboundHandlerAdapter {

  override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit =
    msg match {
      case headers: HttpRequest => super.write(ctx, headers, mapWriteExceptions(ctx, promise))
      case _ => super.write(ctx, msg, promise)
    }

  private[this] def mapWriteExceptions(
    ctx: ChannelHandlerContext,
    promise: ChannelPromise
  ): ChannelPromise = {
    val p = ctx.newPromise()
    p.addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) promise.setSuccess()
        else if (future.isCancelled) promise.cancel(true)
        else
          future.cause match {
            case _: HeaderListSizeException =>
              // In this case we want to synthesize a 431 response and close down the channel.
              ctx.fireChannelRead(headerListSizeResponse())
              ctx.channel.close(promise)
            case other => promise.setFailure(other)

          }
      }
    })
    p
  }

  private[this] def headerListSizeResponse(): FullHttpResponse =
    Bijections.finagle.fullResponseToNetty(BadRequestResponse.headerTooLong())
}
