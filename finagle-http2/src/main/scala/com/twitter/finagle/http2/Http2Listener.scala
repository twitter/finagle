package com.twitter.finagle.http2

import com.twitter.finagle.http
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.netty4.http.exp.{HttpCodecName, initServer}
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.Transport
import io.netty.channel.{ChannelInitializer, Channel, ChannelPipeline}
import io.netty.handler.codec.http.HttpServerCodec

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[finagle] object Http2Listener {

  private[this] def sourceCodec(params: Stack.Params) = {
    val maxInitialLineSize = params[http.param.MaxInitialLineSize].size
    val maxHeaderSize = params[http.param.MaxHeaderSize].size
    val maxRequestSize = params[http.param.MaxRequestSize].size

    new HttpServerCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      maxRequestSize.inBytes.toInt
    )
  }

  private[this] def cleartextListener[In, Out](params: Stack.Params)
    (implicit mIn: Manifest[In], mOut: Manifest[Out]): Listener[In, Out] = {
    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        val source = sourceCodec(params)
        pipeline.addLast(HttpCodecName, source)
        initServer(params)(pipeline)
      },
      params = params,
      setupMarshalling = {
        init: ChannelInitializer[Channel] =>
          new Http2CleartextServerInitializer(init, params)
      }
    )
  }

  private[this] def tlsListener[In, Out](params: Stack.Params)
    (implicit mIn: Manifest[In], mOut: Manifest[Out]): Listener[In, Out] = {
    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        pipeline.addLast(HttpCodecName, sourceCodec(params))
        initServer(params)(pipeline)
      },
      params = params,
      setupMarshalling = {
        init: ChannelInitializer[Channel] => new Http2TlsServerInitializer(init, params)
      }
    )
  }

  def apply[In, Out](params: Stack.Params)
    (implicit mIn: Manifest[In], mOut: Manifest[Out]): Listener[In, Out] = {
    val Transport.ServerSsl(configuration) = params[Transport.ServerSsl]

    if (configuration.isDefined) tlsListener(params)
    else cleartextListener(params)
  }
}
