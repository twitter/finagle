package com.twitter.finagle.http2.transport.server

import com.twitter.finagle.netty4.http.util.UriUtils
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http2.{
  DefaultHttp2Headers,
  DefaultHttp2HeadersFrame,
  Http2HeadersFrame
}
import io.netty.util.ReferenceCountUtil

/**
 * HTTP URI validation that acts upon [[Http2HeadersFrame]] messages in the Netty HTTP/2 pipeline.
 *
 * @see [[com.twitter.finagle.netty4.http.handler.UriValidatorHandler]] for HTTP 1.1 handling
 */
@Sharable
final private[http2] object H2UriValidatorHandler extends ChannelInboundHandlerAdapter {

  val HandlerName: String = "h2UriValidatorHandler"

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case headers: Http2HeadersFrame if !UriUtils.isValidUri(headers.headers().path()) =>
      ReferenceCountUtil.release(msg)

      // If the URI isn't valid, we want to retain consistency between our HTTP/2 and HTTP/1
      // pipelines by returning a 400 Bad Request response instead of continuing down the
      // Netty pipeline, which has inconsistent behavior
      val headers = new DefaultHttp2Headers()
      headers.status(HttpResponseStatus.BAD_REQUEST.codeAsText())
      val frame = new DefaultHttp2HeadersFrame(headers, /* endStream*/ true)
      ctx.writeAndFlush(frame)
    case _ =>
      ctx.fireChannelRead(msg)
  }
}
