package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http.{BadRequestResponse, Response}
import com.twitter.finagle.netty4.http.Bijections
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.TooLongFrameException
import io.netty.handler.codec.http.HttpMessage
import io.netty.util.ReferenceCountUtil

/**
 * A `ChannelHandler` for turning http decoding errors into meaningful responses.
 *
 * The only errors that we explicitly handle are from the  HttpObjectDecoder, specifically the
 * request line and header lengths. A message chunk aggregator, if it exists, handles its own
 * errors. When an error is observed, we generate an appropriate response and close the pipeline
 * after the write operation is complete.
 */
@Sharable
private[netty4] object BadRequestHandler
    extends SimpleChannelInboundHandler[HttpMessage](false /* auto release */ ) {
  def channelRead0(ctx: ChannelHandlerContext, msg: HttpMessage): Unit = {
    if (!msg.decoderResult().isFailure) ctx.fireChannelRead(msg)
    else {
      ReferenceCountUtil.release(msg)
      handleException(ctx, msg.decoderResult().cause())
    }
  }

  private[this] def handleException(ctx: ChannelHandlerContext, ex: Throwable): Unit = {
    val resp = exceptionToResponse(ex)
    val nettyResp = Bijections.finagle.fullResponseToNetty(resp)

    // We must close the connection on these types of failures since
    // the inbound TCP stream is in an undefined state
    ctx
      .writeAndFlush(nettyResp)
      .addListener(ChannelFutureListener.CLOSE)
  }

  private[this] def exceptionToResponse(ex: Throwable): Response = ex match {
    case ex: TooLongFrameException =>
      // Chunk aggregation is already handled by the HttpObjectAggregator, so we only need
      // to consider the request line and headers
      if (ex.getMessage.startsWith("An HTTP line is larger than "))
        BadRequestResponse.uriTooLong()
      else
        BadRequestResponse.headerTooLong()
    case _ =>
      BadRequestResponse()
  }
}
