package com.twitter.finagle.http2.transport.server

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.common.H2StreamChannelInit
import com.twitter.finagle.http2.{Http2PipelineInitializer, MultiplexHandlerBuilder}
import com.twitter.finagle.netty4.http._
import com.twitter.finagle.param.Stats
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInitializer}
import io.netty.handler.ssl.{ApplicationProtocolNames, ApplicationProtocolNegotiationHandler}

final private[http2] class ServerNpnOrAlpnHandler(
  init: ChannelInitializer[Channel],
  params: Stack.Params)
    extends ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  @throws(classOf[Exception])
  protected def configurePipeline(ctx: ChannelHandlerContext, protocol: String): Unit =
    protocol match {
      case ApplicationProtocolNames.HTTP_2 =>
        // Http2 has been negotiated, replace the HttpCodec with an Http2Codec
        upgradeCounter.incr()
        ctx.channel.config.setAutoRead(true)
        val initializer = H2StreamChannelInit.initServer(init, params)
        val (codec, handler) = MultiplexHandlerBuilder.serverFrameCodec(params, initializer)
        MultiplexHandlerBuilder.addStreamsGauge(statsReceiver, codec, ctx.channel)
        ctx.pipeline.replace(HttpCodecName, Http2CodecName, codec)
        ctx.pipeline.addAfter(Http2CodecName, Http2MultiplexHandlerName, handler)

        Http2PipelineInitializer.setupServerPipeline(ctx.pipeline, params)

      case ApplicationProtocolNames.HTTP_1_1 =>
      // The Http codec is already in the pipeline, so we are good!
      case _ =>
        throw new IllegalStateException("unknown protocol: " + protocol)
    }
}
