package com.twitter.finagle.http2.transport

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.http
import com.twitter.finagle.netty4.param.Allocator
import io.netty.channel._
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec

private[http2] object H2Init {
  def apply(init: ChannelInitializer[Channel], params: Stack.Params): ChannelInitializer[Channel] =
    new ChannelInitializer[Channel] {
      def initChannel(ch: Channel): Unit = {
        val alloc = params[Allocator].allocator
        ch.config.setAllocator(alloc)
        ch.pipeline.addLast(new Http2NackHandler)
        ch.pipeline.addLast(new Http2StreamFrameToHttpObjectCodec(
          true /* isServer */,
          false /* validateHeaders */
        ))
        ch.pipeline.addLast(StripHeadersHandler.HandlerName, StripHeadersHandler)
        ch.pipeline.addLast(new RstHandler)
        http.initServer(params)(ch.pipeline)
        ch.pipeline.addLast(init)
      }
    }
}
