package com.twitter.finagle.http2.transport

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.MultiplexCodecBuilder
import com.twitter.finagle.netty4.http._
import com.twitter.finagle.param.{Stats, Timer => TimerParam}
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
        val http2MultiplexCodec = MultiplexCodecBuilder.serverMultiplexCodec(params, initializer)
        MultiplexCodecBuilder.addStreamsGauge(statsReceiver, http2MultiplexCodec, ctx.channel)
        ctx.pipeline.replace(HttpCodecName, Http2CodecName, http2MultiplexCodec)
        val timer = params[TimerParam].timer
        ctx.pipeline.addAfter(Http2CodecName, H2Filter.HandlerName, new H2Filter(timer))

      case ApplicationProtocolNames.HTTP_1_1 =>
      // The Http codec is already in the pipeline, so we are good!
      case _ =>
        throw new IllegalStateException("unknown protocol: " + protocol)
    }
}
