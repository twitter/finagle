package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.netty4.http.exp.initServer
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.{TlsConfig, Transport}
import io.netty.channel.{ChannelInitializer, Channel, ChannelPipeline, ChannelDuplexHandler}
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.codec.http2.{Http2Codec, Http2ServerDowngrader}

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  val PlaceholderName = "placeholder"

  private[this] def priorKnowledgeListener[In, Out](params: Stack.Params): Listener[In, Out] =
    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        // we inject a dummy handler so we can replace it with the real stuff
        // after we get `init` in the setupMarshalling phase.
        pipeline.addLast(PlaceholderName, new ChannelDuplexHandler(){})
      },
      params = params + Netty4Listener.BackPressure(false),
      setupMarshalling = { init: ChannelInitializer[Channel] =>
        val initializer = new ChannelInitializer[Channel] {
          def initChannel(ch: Channel): Unit = {
            // downgrade from http/2 to http/1.1 types
            ch.pipeline.addLast(new Http2ServerDowngrader(false /* validateHeaders */))
            initServer(params)(ch.pipeline)
            ch.pipeline.addLast(init)
          }
        }
        new ChannelInitializer[Channel] {
          def initChannel(ch: Channel): Unit = {
            ch.pipeline.replace(PlaceholderName, "http2Codec", new Http2Codec(true, initializer))
          }
        }
      }
    )


  private[this] def sourceCodec(params: Stack.Params) = {
    val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size
    val maxHeaderSize = params[httpparam.MaxHeaderSize].size
    val maxRequestSize = params[httpparam.MaxRequestSize].size

    new HttpServerCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      maxRequestSize.inBytes.toInt
    )
  }

  private[this] def cleartextListener[In, Out](params: Stack.Params): Listener[In, Out] = {
    val source = sourceCodec(params)
    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        pipeline.addLast("httpCodec", source)
        initServer(params)(pipeline)
      },
      params = params,
      setupMarshalling = {
        init: ChannelInitializer[Channel] =>
          new Http2CleartextServerInitializer(init, params, source)
      }
    )
  }

  private[this] def tlsListener[In, Out](params: Stack.Params): Listener[In, Out] = {
    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        pipeline.addLast("httpCodec", sourceCodec(params))
        initServer(params)(pipeline)
      },
      params = params,
      setupMarshalling = {
        init: ChannelInitializer[Channel] => new Http2TlsServerInitializer(init, params)
      }
    )
  }

  def apply[In, Out](params: Stack.Params): Listener[In, Out] = {
    val PriorKnowledge(priorKnowledge) = params[PriorKnowledge]
    val Transport.Tls(tlsConfig) = params[Transport.Tls]


    if (tlsConfig != TlsConfig.Disabled) tlsListener(params)
    else if (priorKnowledge) priorKnowledgeListener(params)
    else cleartextListener(params)
  }
}
