package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.server.H2ServerFilter
import com.twitter.finagle.netty4.http.handler.UriValidatorHandler
import com.twitter.finagle.param.Timer
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http2.Http2MultiplexHandler

private[http2] object Http2PipelineInitializer {

  /**
   * Install Finagle specific filters and handlers common across all HTTP/2 only pipelines
   *
   * @param ctx
   * @param params
   */
  def setup(ctx: ChannelHandlerContext, params: Stack.Params): Unit = {
    // we insert immediately after the Http2MultiplexHandler#0, which we know are the
    // last Http2 frames before they're converted to Http/1.1
    val timer = params[Timer].timer

    val codecName = ctx.pipeline
      .context(classOf[Http2MultiplexHandler])
      .name

    ctx.pipeline
      .addAfter(codecName, H2ServerFilter.HandlerName, new H2ServerFilter(timer))
      .remove(UriValidatorHandler.HandlerName)
  }
}
