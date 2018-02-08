package com.twitter.finagle.http2.transport

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.param.{EncoderIgnoreMaxHeaderListSize, FrameLoggerNamePrefix}
import com.twitter.finagle.http2.{LoggerPerFrameTypeLogger, Settings}
import com.twitter.finagle.netty4.http._
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.Gauge
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelHandlerContext, ChannelInitializer}
import io.netty.handler.codec.http2.{Http2MultiplexCodec, Http2MultiplexCodecBuilder}
import io.netty.handler.ssl.{ApplicationProtocolNames, ApplicationProtocolNegotiationHandler}
import io.netty.util.AttributeKey

final private[http2] class NpnOrAlpnHandler(init: ChannelInitializer[Channel], params: Stack.Params)
    extends ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  @throws(classOf[Exception])
  protected def configurePipeline(ctx: ChannelHandlerContext, protocol: String): Unit = protocol match {
    case ApplicationProtocolNames.HTTP_2 =>
      // Http2 has been negotiated, replace the HttpCodec with an Http2Codec
      val initializer = H2Init(init, params)
      upgradeCounter.incr()

      ctx.channel.config.setAutoRead(true)
      val initialSettings = Settings.fromParams(params, isServer = true)
      val logger = new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix)

      val codec: Http2MultiplexCodec = Http2MultiplexCodecBuilder.forServer(initializer)
        .frameLogger(logger)
        .initialSettings(initialSettings)
        .encoderIgnoreMaxHeaderListSize(params[EncoderIgnoreMaxHeaderListSize].ignoreMaxHeaderListSize)
        .build()

      val listener = codec.decoder.frameListener
      codec.decoder.frameListener(new PingListenerDecorator(listener))

      val streams = statsReceiver.addGauge("streams") { codec.connection.numActiveStreams }

      // We're attaching a gauge to the channel's attributes to make sure it stays referenced
      // as long as channel is alive.
      ctx.channel.attr(AttributeKey.valueOf[Gauge]("streams_gauge")).set(streams)

      // We're removing the gauge on channel closure.
      ctx.channel.closeFuture.addListener(new ChannelFutureListener {
        override def operationComplete(f: ChannelFuture): Unit = streams.remove()
      })

      ctx.pipeline.replace(HttpCodecName, Http2CodecName, codec)

    case ApplicationProtocolNames.HTTP_1_1 =>
    // The Http codec is already in the pipeline, so we are good!
    case _ =>
      throw new IllegalStateException("unknown protocol: " + protocol)
  }
}
