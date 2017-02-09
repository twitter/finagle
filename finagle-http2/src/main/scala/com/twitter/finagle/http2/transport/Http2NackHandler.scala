package com.twitter.finagle.http2.transport

import com.twitter.finagle.http.filter.HttpNackFilter.{RetryableNackHeader, NonRetryableNackHeader}
import io.netty.channel.{ChannelOutboundHandlerAdapter, ChannelHandlerContext, ChannelPromise, ChannelPromiseNotifier}
import io.netty.handler.codec.http2.{
  DefaultHttp2ResetFrame,
  Http2Error,
  Http2HeadersFrame,
  Http2ResetFrame
}

/**
 * Converts finagle's nack-via headers messages into true NACKs using HTTP/2
 * RSTs.
 *
 * Nonretryable nacks are represented as ENHANCE_YOUR_CALM (0xB) and retryable
 * nacks are represented as REFUSED_STREAM (0x7).
 *
 * This also swallows frames after the headers, since we also include a failure
 * string that isn't easy to pass along using an RST.
 */
private[http2] class Http2NackHandler extends ChannelOutboundHandlerAdapter {

  // this is thread safe because netty guarantees that each handler is single
  // threaded.
  private[this] var continue: ChannelPromise = null

  override def write(
    ctx: ChannelHandlerContext,
    msg: Object,
    p: ChannelPromise
  ): Unit = if (continue == null) {
    msg match {
      case headersFrame: Http2HeadersFrame =>
        val headers = headersFrame.headers
        if (headers.contains(RetryableNackHeader)) {
          continue = p
          super.write(ctx, Http2NackHandler.retryableNack, p)
        } else if (headers.contains(NonRetryableNackHeader)) {
          continue = p
          super.write(ctx, Http2NackHandler.nonRetryableNack, p)
        } else {
          super.write(ctx, msg, p)
        }
      case _ => super.write(ctx, msg, p)
    }
  } else {
    val listener = new ChannelPromiseNotifier(p)
    continue.addListener(listener)
  }
}

private[transport] object Http2NackHandler {
  // these cannot be reused because they're mutated on use.
  def retryableNack: Http2ResetFrame = new DefaultHttp2ResetFrame(Http2Error.REFUSED_STREAM)
  def nonRetryableNack: Http2ResetFrame = new DefaultHttp2ResetFrame(Http2Error.ENHANCE_YOUR_CALM)
}
