package com.twitter.finagle.http2

import com.twitter.finagle.http
import com.twitter.finagle.Stack
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.finagle.netty4.{DirectToHeapInboundHandlerName, Netty4Listener}
import com.twitter.finagle.netty4.channel.DirectToHeapInboundHandler
import com.twitter.finagle.netty4.http.exp.{HttpCodecName, initServer}
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.{TlsConfig, Transport}
import io.netty.channel.{ChannelInitializer, Channel, ChannelPipeline,
  ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.codec.http2.{Http2Codec, Http2ServerDowngrader, Http2ResetFrame}

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[http2] object Http2Listener {
  val PlaceholderName = "placeholder"

  private[this] def priorKnowledgeListener[In, Out](params: Stack.Params): Listener[In, Out] =
    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        pipeline.addLast(DirectToHeapInboundHandlerName, DirectToHeapInboundHandler)
        // we inject a dummy handler so we can replace it with the real stuff
        // after we get `init` in the setupMarshalling phase.
        pipeline.addLast(PlaceholderName, new ChannelInboundHandlerAdapter(){})
      },
      params = params + Netty4Listener.BackPressure(false),
      setupMarshalling = { init: ChannelInitializer[Channel] =>
        val initializer = new ChannelInitializer[Channel] {
          def initChannel(ch: Channel): Unit = {
            // downgrade from http/2 to http/1.1 types
            ch.pipeline.addLast(new Http2ServerDowngrader(false /* validateHeaders */))
            // TODO: send an interrupt instead of dropping the reset frame
            // we want to drop reset frames because the Http2ServerDowngrader doesn't know what to
            // do with them, and our dispatchers expect to only get http/1.1 message types.
            ch.pipeline.addLast(new ChannelInboundHandlerAdapter() {
              override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
                if (!msg.isInstanceOf[Http2ResetFrame])
                  super.channelRead(ctx, msg)
              }
            })
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
    val maxInitialLineSize = params[http.param.MaxInitialLineSize].size
    val maxHeaderSize = params[http.param.MaxHeaderSize].size
    val maxRequestSize = params[http.param.MaxRequestSize].size

    new HttpServerCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      maxRequestSize.inBytes.toInt
    )
  }

  private[this] def cleartextListener[In, Out](params: Stack.Params): Listener[In, Out] = {
    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        pipeline.addLast(DirectToHeapInboundHandlerName, DirectToHeapInboundHandler)
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

  private[this] def tlsListener[In, Out](params: Stack.Params): Listener[In, Out] = {
    Netty4Listener(
      pipelineInit = { pipeline: ChannelPipeline =>
        pipeline.addLast(DirectToHeapInboundHandlerName, DirectToHeapInboundHandler)
        pipeline.addLast(HttpCodecName, sourceCodec(params))
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
