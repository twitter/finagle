package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.netty4.http.exp.initServer
import com.twitter.finagle.server.Listener
import io.netty.channel.{ChannelInitializer, Channel, ChannelPipeline}
import io.netty.handler.codec.http.HttpServerCodec

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  def apply[In, Out](params: Stack.Params): Listener[In, Out] = {
    val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size
    val maxHeaderSize = params[httpparam.MaxHeaderSize].size
    val maxRequestSize = params[httpparam.MaxRequestSize].size

    val sourceCodec = new HttpServerCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      maxRequestSize.inBytes.toInt
    )

    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        pipeline.addLast("httpCodec", sourceCodec)
        initServer(params)(pipeline)
      },
      params = params,
      setupMarshalling = { init: ChannelInitializer[Channel] =>
        new Http2ServerInitializer(init, params, sourceCodec)
      }
    )
  }
}
