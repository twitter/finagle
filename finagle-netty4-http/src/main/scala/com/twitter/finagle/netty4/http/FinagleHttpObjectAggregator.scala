package com.twitter.finagle.netty4.http

import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.http.{HttpMessage, HttpObjectAggregator, HttpResponse}
import io.netty.handler.codec.http.HttpHeaderNames.EXPECT
import io.netty.handler.codec.http.HttpResponseStatus.CONTINUE

/**
 * This variant of netty's [[HttpObjectAggregator]] is distinguished by the
 * ability to ignore and propagate 'expect' headers or strip them after a
 * 100 CONTINUE response is sent.
 */
private[netty4] class FinagleHttpObjectAggregator(
  maxContentLength: Int,
  handleExpectContinue: Boolean
) extends HttpObjectAggregator(maxContentLength) {

  override def newContinueResponse(
    start: HttpMessage,
    maxContentLength: Int,
    pipeline: ChannelPipeline
  ): AnyRef = {
    if (handleExpectContinue) {
      // this header stripping logic can be removed after
      // https://github.com/netty/netty/issues/7075 is resolved
      val res = super.newContinueResponse(start, maxContentLength, pipeline)
      res match {
        case cont: HttpResponse if cont.status() == CONTINUE =>
          start.headers.remove(EXPECT)
        case _ =>
          ()
      }

      res
    } else null
  }
}
