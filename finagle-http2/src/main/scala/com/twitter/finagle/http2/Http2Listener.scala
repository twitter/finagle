package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.netty4.http.exp.initServer
import com.twitter.finagle.server.Listener
import io.netty.channel.{ChannelInitializer, Channel}

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  def apply[In, Out](params: Stack.Params): Listener[In, Out] = Netty4Listener(
    pipelineInit = initServer(params),
    params = params,
    handlerDecorator = { init: ChannelInitializer[Channel] => new Http2ServerInitializer(init, params) })
}
