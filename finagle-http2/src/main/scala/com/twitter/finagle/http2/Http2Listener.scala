package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.server.Listener
import io.netty.channel.{ChannelHandler, ChannelPipeline}

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  private[this] val init: ChannelPipeline => Unit = { pipeline: ChannelPipeline =>
    pipeline.addLast(new Http2ServerDowngrader())
  }

  def apply[In, Out](params: Stack.Params): Listener[In, Out] = {
    Netty4Listener(params, init, handlerDecorator = { handler: ChannelHandler =>
      new Http2ServerInitializer(handler)
    })
  }
}
