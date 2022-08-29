package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http.headers.Rfc7230HeaderValidation
import com.twitter.finagle.http.BadRequestResponse
import com.twitter.finagle.http.Response
import com.twitter.finagle.netty4.http.Bijections
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.handler.codec.TooLongFrameException
import io.netty.handler.codec.http.HttpMessage
import io.netty.handler.codec.http.HttpObject
import io.netty.util.ReferenceCountUtil

object BadRequestHandler {
  val log = Logger()
  val HandlerName: String = "badRequestHandler"
}

/**
 * A `ChannelHandler` for turning http decoding errors into meaningful responses.
 *
 * The only errors that we explicitly handle are from the  HttpObjectDecoder, specifically the
 * request line and header lengths. A message chunk aggregator, if it exists, handles its own
 * errors. When an error is observed, we generate an appropriate response and close the pipeline
 * after the write operation is complete.
 *
 * @see [[ClientExceptionMapper]] for a client-side implementation of this handler
 */
@Sharable
private[netty4] class BadRequestHandler(stats: StatsReceiver) extends ChannelInboundHandlerAdapter {

  private[this] val invalidHeaderNames = stats.counter("rejected_invalid_header_names")
  private[this] val invalidHeaderValues = stats.counter("rejected_invalid_header_values")

  import BadRequestHandler._

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    // NOTE: HttpObjectDecoder sets the DecoderResult on an inbound HTTP message when either
    // headers or initial line exceed a desired limit.
    case obj: HttpObject if obj.decoderResult.isFailure =>
      ReferenceCountUtil.release(obj)
      handleDecodeFailure(ctx, obj)

    case _ =>
      ctx.fireChannelRead(msg)
  }

  private[this] def handleDecodeFailure(ctx: ChannelHandlerContext, obj: HttpObject): Unit =
    obj match {
      case _: HttpMessage =>
        // We detected a decode failure in the message's initial line. We can reply back safely.
        val failure = obj.decoderResult.cause
        val resp = exceptionToResponse(failure)
        val nettyResp = Bijections.finagle.fullResponseToNetty(resp)

        failure match {
          case _: Rfc7230HeaderValidation.NameValidationException =>
            invalidHeaderNames.incr()
          case _: Rfc7230HeaderValidation.ValueValidationException =>
            invalidHeaderValues.incr()
          case _ =>
        }

        // We must close the connection on these types of failures since
        // the inbound TCP stream is in an undefined state
        ctx
          .writeAndFlush(nettyResp)
          .addListener(ChannelFutureListener.CLOSE)
      case _ =>
        // A decode failure must be from validating trailers in the last chunk. We can't really
        // reply anything as we're not sure if application-logic already replied. The only
        // meaningful thing we can do is to hang up.

        log.debug(
          "Detected invalid trailing headers in the HTTP stream. Tearing down the connection.")

        ctx.close()
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
