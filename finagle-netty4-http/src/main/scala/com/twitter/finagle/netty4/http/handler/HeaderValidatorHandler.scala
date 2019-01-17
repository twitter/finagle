package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http.Rfc7230HeaderValidation
import com.twitter.finagle.http.Rfc7230HeaderValidation.{
  ObsFoldDetected,
  ValidationFailure,
  ValidationSuccess
}
import com.twitter.util.{Return, Throw, Try}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.DecoderResult
import io.netty.handler.codec.http.{HttpHeaders, HttpMessage}

@Sharable
private[netty4] object HeaderValidatorHandler extends ChannelInboundHandlerAdapter {

  val HandlerName: String = "headerValidationHandler"

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = msg match {
    case msg: HttpMessage => validateMessage(ctx, msg)
    case _ => super.channelRead(ctx, msg)
  }

  private[this] def validateMessage(ctx: ChannelHandlerContext, msg: HttpMessage): Unit = {
    validateHeaders(msg.headers) match {
      case Return(_) => () // nop
      case Throw(ex) =>
        // At least one header is invalid. Since we didn't check them all we clear all of them
        // just in case there are multiple invalid headers. The message shouldn't be used
        // so clearing the header map is just to be super safe.
        msg.headers.clear()
        msg.setDecoderResult(DecoderResult.failure(ex))
    }
    ctx.fireChannelRead(msg)
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
