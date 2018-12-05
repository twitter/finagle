package com.twitter.finagle.netty4.http

import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.http.{HttpMessage, HttpObjectAggregator}

/**
 * This variant of netty's [[HttpObjectAggregator]] is distinguished by the
 * ability to ignore and propagate 'expect' headers or strip them after a
 * 100 CONTINUE response is sent.
 */
private[netty4] class FinagleHttpObjectAggregator(
  maxContentLength: Int,
  handleExpectContinue: Boolean)
    extends HttpObjectAggregator(maxContentLength) {

  override def newContinueResponse(
    start: HttpMessage,
    maxContentLength: Int,
    pipeline: ChannelPipeline
  ): AnyRef = {
    if (handleExpectContinue) super.newContinueResponse(start, maxContentLength, pipeline)
    else null
  }
}
