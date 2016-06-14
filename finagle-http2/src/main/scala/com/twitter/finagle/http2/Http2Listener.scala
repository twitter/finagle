package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.server.Listener
import io.netty.channel.{ChannelPipeline, ChannelInitializer, Channel}
import io.netty.handler.codec.http2.Http2ServerDowngrader

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  private[this] val init: ChannelPipeline => Unit = { pipeline: ChannelPipeline =>
    pipeline.addLast(new Http2ServerDowngrader(false /*validateHeaders*/))
  }

  def apply[In, Out](params: Stack.Params): Listener[In, Out] = Netty4Listener(
    pipelineInit = init,
    // we turn off backpressure because Http2 only works with autoread on for now
    params = params + Netty4Listener.BackPressure(false),
    handlerDecorator = { init: ChannelInitializer[Channel] => new Http2ServerInitializer(init, params) })
}
