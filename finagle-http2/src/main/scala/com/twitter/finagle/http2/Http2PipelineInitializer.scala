package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.{H2Filter, H2UriValidatorHandler}
import com.twitter.finagle.netty4.http.handler.UriValidatorHandler
import com.twitter.finagle.param.Timer
import io.netty.channel.ChannelHandlerContext

private[http2] object Http2PipelineInitializer {

  /**
   * Install Finagle specific filters and handlers common across all HTTP/2 only pipelines
   *
   * @param ctx
   * @param params
   * @param codecName The name of the handler where the remaining handlers will be added after
   */
  def setup(ctx: ChannelHandlerContext, params: Stack.Params, codecName: String): Unit = {
    // we insert immediately after the Http2MultiplexCodec#0, which we know are the
    // last Http2 frames before they're converted to Http/1.1
    val timer = params[Timer].timer
    ctx.pipeline
      .addAfter(codecName, H2Filter.HandlerName, new H2Filter(timer))
    ctx.pipeline
      .addAfter(H2Filter.HandlerName, H2UriValidatorHandler.HandlerName, H2UriValidatorHandler)

    ctx.pipeline
      .remove(UriValidatorHandler.HandlerName)

  }

}
