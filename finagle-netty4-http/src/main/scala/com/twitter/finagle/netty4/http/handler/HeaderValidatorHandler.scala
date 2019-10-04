package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http.headers.Rfc7230HeaderValidation
import com.twitter.finagle.http.headers.Rfc7230HeaderValidation.{
  ObsFoldDetected,
  ValidationFailure,
  ValidationSuccess
}
import com.twitter.util.{Return, Throw, Try}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.DecoderResult
import io.netty.handler.codec.http.{
  FullHttpMessage,
  HttpHeaders,
  HttpMessage,
  HttpObject,
  LastHttpContent
}

/**
 * HTTP headers validation in Finagle is done in two different places, depending on whether it's
 * inbound or outbound traffic. All inbound headers are validated in the Netty pipeline so we can
 * reject malformed requests earlier, before they enter the Finagle land. This pipeline handler
 * does exactly this. Similar to other Netty components, it sets HTTP object's `DecoderResult` to a
 * failure so the next handler(s) can take an appropriate action (reject if it's a server; convert
 * to an exception if it's a client).
 */
@Sharable
private[netty4] object HeaderValidatorHandler extends ChannelInboundHandlerAdapter {

  val HandlerName: String = "headerValidationHandler"

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    msg match {
      case msg: FullHttpMessage =>
        // HeaderValidationHandler is installed after stream-aggregating handlers in our pipelines,
        // which means we can see full messages here. Full messages have both headers and trailers.
        validateObject(ctx, msg, msg.headers)
        validateObject(ctx, msg, msg.trailingHeaders)

      case msg: HttpMessage =>
        validateObject(ctx, msg, msg.headers)

      case msg: LastHttpContent =>
        validateObject(ctx, msg, msg.trailingHeaders)

      case _ => ()
    }

    ctx.fireChannelRead(msg)
  }

  private[this] def validateObject(
    ctx: ChannelHandlerContext,
    obj: HttpObject,
    headers: HttpHeaders
  ): Unit = validateHeaders(headers) match {
    case Return(_) => () // nop
    case Throw(ex) =>
      // At least one header is invalid. Since we didn't check them all we clear all of them
      // just in case there are multiple invalid headers. The message shouldn't be used
      // so clearing the header map is just to be super safe.
      headers.clear()
      obj.setDecoderResult(DecoderResult.failure(ex))
  }

  private[this] def validateHeaders(headers: HttpHeaders): Try[Unit] = {
    val it = headers.iteratorCharSequence()
    while (it.hasNext) {
      val header = it.next()
      val name = header.getKey
      val value = header.getValue
      Rfc7230HeaderValidation.validateName(name) match {
        case ValidationFailure(e) => return Throw(e)
        case ValidationSuccess => // () nop
      }

      Rfc7230HeaderValidation.validateValue(name, value) match {
        case ValidationFailure(e) => return Throw(e)
        case ObsFoldDetected =>
          header.setValue(Rfc7230HeaderValidation.replaceObsFold(value))
        case ValidationSuccess => () // nop
      }
    }

    // If we get here everything was validated.
    Try.Unit
  }
}
